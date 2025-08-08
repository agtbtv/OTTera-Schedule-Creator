import os
import re
import io
import threading
import requests
import pandas as pd
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from dotenv import load_dotenv
from pathlib import Path
from datetime import datetime, timedelta

# Load environment variables from .env file
load_dotenv()

# Initialize the Slack Bolt app
app = App(
    token=os.environ["SLACK_BOT_TOKEN"],
    signing_secret=os.environ["SLACK_SIGNING_SECRET"]
)

# Channel Config
CHANNEL_CONFIG = {
    "ACL": {
        "spreadsheet_id": "1iAnFLY7npmqf-fpY0Odw7ugvBBU8k_UrM06KLt0maw4",
        "linear_channel_id": 2802,
        "house_code_pattern": r'(CORN\d+|CORNFILL\d+|AROUND_THE_ACL_\d+\.\d+\.\d+)',
        "bumper_pattern": r'(ACLBUMP\d+)',
        "output_prefix": "ACL",
        "processing_logic": "standard"
    },
    "Bark": {
        "spreadsheet_id": "1jfZJjaA8oDSbFfInDwxQfvEEFTPWT5L58vXujBuwptw",
        "linear_channel_id": 850,
        "house_code_pattern": r'(BARK\d+|BARKFILL\d+)',
        "bumper_pattern": r'(BARKBUMP\d+)',
        "output_prefix": "BarkTV",
        "processing_logic": "standard"
    },
    "Billiard": {
        "spreadsheet_id": '1Y6Y6OsYEj0d0jEOgyU4E7gw-PelUdNMKl9VWjvKTlF0',
        "linear_channel_id": 178,
        "house_code_pattern": r'(BILL\d+|BILLFILL\d+)',
        "bumper_pattern": r'(BILLBUMP\d+)',
        "output_prefix": "BilliardTV",
        "processing_logic": "standard"
    },
    "Boxing": {
        "spreadsheet_id": '1jdMKwExqP3g0KpmCTrbOdxS74eAHLaeIsZPb-00NgT0',
        "linear_channel_id": 2797,
        "house_code_pattern": r'(BOX\d+|BOXFILL\d+)',
        "bumper_pattern": r'(BOXBUMP\d+)',
        "output_prefix": "BoxingTV",
        "processing_logic": "standard"
    },
    "PLL Domestic": {
        "spreadsheet_id": '1qLC9nSmQHB7pd8lIEe6NXyQzs_49mSnWv53I4cq6EcQ',
        "linear_channel_id": 9,
        "house_code_pattern": r'(MPLS_EP\d+|PLL\d+|PLLFILL\d+|MPLS\d+)',
        "bumper_pattern": r'(PLLBUMP\d+)',
        "hourly_promo_in": "6139",
        "hourly_promo_out": "6336",
        "output_prefix": "PLL_Dom",
        "processing_logic": "pll domestic"
    },
    "PLL International": {
        "spreadsheet_id": '1qLC9nSmQHB7pd8lIEe6NXyQzs_49mSnWv53I4cq6EcQ',
        "linear_channel_id": 176,
        "house_code_pattern": r'(MPLS_EP\d+|PLL\d+|PLLFILL\d+|MPLS\d+)',
        "bumper_pattern": r'(PLLBUMP\d+)',
        "hourly_promo_in": "6139",
        "hourly_promo_out": "6336",
        "output_prefix": "PLL_Int",
        "processing_logic": "standard"
    },
    "PowerSports World": {
        "spreadsheet_id": '116ZbKMMQxROJX3YjFyxtauhFVkx5GgcHeSBYLk78GJg',
        "linear_channel_id": 2800,
        "house_code_pattern": r'(PSW\d+|PSWFILL\d+)',
        "bumper_pattern": r'(PSWBUMP\d+)',
        "output_prefix": "PSW",
        "processing_logic": "standard"
    },
}

# --- This is your existing logic, modified to work with Slack ---
class ProcessingEngine:
    def __init__(self, config, input_date_str, library_file_content, client, channel_id, thread_ts):
        self.config = config
        self.input_date_str = input_date_str
        self.library_file_content = library_file_content
        self.client = client
        self.channel_id = channel_id
        self.thread_ts = thread_ts
        self.unmatched_ids = []
        self.premature_mpls = []

    def log(self, message):
        """Sends a log message to the Slack thread."""
        try:
            self.client.chat_postMessage(
                channel=self.channel_id,
                thread_ts=self.thread_ts,
                text=message
            )
        except Exception as e:
            print(f"Error logging to Slack: {e}")

    def run(self):
        try:
            # Step 1: Date processing
            week_name, input_date = self._get_week_name_of_input_date(self.input_date_str)
            if not week_name: return

            # Step 2: Download Google Sheet
            downloads_folder = str(Path.home() / "Downloads")
            temp_grid_file = os.path.join(downloads_folder, f"temp_grid_{self.config['output_prefix']}.csv")
            if not self._download_sheet(self.config['spreadsheet_id'], week_name.upper(), temp_grid_file): return

            # Step 3: Process downloaded grid
            grid_data = self._prepare_grid_data(temp_grid_file, input_date)
            
            # Step 4: Extract programming
            if self.config.get('processing_logic') == 'pll domestic':
                programming_df = self._process_show_programming_pll_domestic(grid_data)
            else:
                programming_df = self._process_show_programming_standard(grid_data)
            
            # Step 5: Load library sheet FROM MEMORY (passed from Slack upload)
            self.log("Getting OTTera node IDs...")
            library_df = self._filter_unique_rows_by_latest_date(self.library_file_content)
            if library_df.empty: return

            # Step 6: Validate and generate final output
            final_df = self._create_final_sheet(programming_df, library_df)

            if final_df is not None:
                # Step 7: UPLOAD the final output file to Slack
                self.log("Success! Creating schedule sheet...")
                output_filename = f"{self.config['output_prefix']}_Schedule_Sheet_{week_name.upper()}.csv"
                
                self.client.files_upload_v2(
                    channel=self.channel_id,
                    thread_ts=self.thread_ts,
                    content=final_df.to_csv(index=False),
                    filename=output_filename,
                    initial_comment=f"Here it is!"
                )
            else:
                self.log(f"--- {self.config['output_prefix']} | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---")
                self.log("🚫 *PROCESS HALTED* due to validation errors found above.")

        except Exception as e:
            self.log(f"\n--- A CRITICAL ERROR OCCURRED ---\n`{e}`")
            import traceback
            self.log(f"```\n{traceback.format_exc()}\n```")
        finally:
            if 'temp_grid_file' in locals() and os.path.exists(temp_grid_file):
                os.remove(temp_grid_file)

    def _filter_unique_rows_by_latest_date(self, csv_content):
        """Modified to read CSV content directly instead of a file path."""
        try:
            df = pd.read_csv(io.StringIO(csv_content))
            if 'legacy_id' not in df.columns or 'id' not in df.columns:
                self.log("ERROR: Library sheet must contain 'legacy_id' and 'id' columns.")
                return pd.DataFrame()
            df_sorted = df.sort_values(by=['legacy_id', 'id'], ascending=[True, False])
            return df_sorted.drop_duplicates(subset=['legacy_id'], keep='first')
        except Exception as e:
            self.log(f"ERROR: Failed to read or process library CSV content: {e}")
            return pd.DataFrame()

    # --- Paste all your other _methods from ProcessingEngine here, unchanged ---
    def _get_week_name_of_input_date(self, input_date_str):
        date_formats = ["%Y-%m-%d", "%m%d%Y", "%m/%d/%Y", "%m-%d-%Y", "%m%d%y", "%m/%d/%y", "%m-%d-%y"]
        input_date = None
        for date_format in date_formats:
            try:
                input_date = datetime.strptime(input_date_str, date_format)
                break
            except ValueError:
                continue
        
        if not input_date:
            self.log(f"ERROR: Invalid date format: {input_date_str}. Please use a valid format.")
            return None, None

        start_of_week = input_date - timedelta(days=input_date.weekday())
        end_of_week = start_of_week + timedelta(days=6)
        week_start_month = start_of_week.strftime("%b")
        week_end_month = end_of_week.strftime("%b")
        week_name = f"{week_start_month} {start_of_week.day}-{week_end_month} {end_of_week.day}"
        return week_name, start_of_week

    def _download_sheet(self, spreadsheet_id, sheet_name, output_file):
        # schedule for '{sheet_name}'...")
        url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"
        try:
            response = requests.get(url, timeout=30)
            if response.status_code == 200:
                with open(output_file, 'wb') as f:
                    f.write(response.content)
                #self.log("Download successful.")
                return True
            else:
                self.log(f"ERROR: Could not get grid for '{sheet_name}'.")
                self.log(f"Status code: {response.status_code}. Response: {response.text[:200]}")
                self.log("Please check if the Google Sheet exists and the tab name is correct.")
                return False
        except requests.exceptions.RequestException as e:
            self.log(f"ERROR: A network error occurred while downloading the sheet: {e}")
            return False

    def _prepare_grid_data(self, file_path, start_date):
        full_grid_data = pd.read_csv(file_path)
        grid_data = full_grid_data.iloc[:50].copy()
        grid_data = grid_data.drop(grid_data.columns[[8]], axis=1, errors='ignore')
        grid_data = grid_data.drop(grid_data.index[:2]).reset_index(drop=True)
        
        week_dates = [(start_date + timedelta(days=i)).strftime("%m/%d/%Y") for i in range(7)]
        week_dates.insert(0, 'Start Time')
        grid_data.columns = week_dates
        
        grid_data['Start Time'] = pd.to_datetime(grid_data['Start Time'], errors='coerce').dt.strftime('%H:%M')
        grid_data = grid_data.fillna('')
        return grid_data
    
    def _process_show_programming_standard(self, grid_data):
        # This function's default is to ALWAYS check for media lists and broken glass,
        #self.log("-> Applying Standard parsing rules.")
        
        results = []
        house_code_pattern = self.config['house_code_pattern']
        bumper_pattern = self.config.get('bumper_pattern')
        media_list_pattern = r'^MEDIA\s?LIST[:\s]*?(\d+)|[^\w\s][\s]*MEDIA\s?LIST[:\s]*?(\d+)|^ML[:\s]*?(\d+)|[^\w\s][\s]*ML[:\s]*?(\d+)'
        qt_media_list_pattern = r'QT\s+MEDIA\s?LIST[:\s]*?(\d+)'

        for col in grid_data.columns[1:8]:
            day_data = grid_data[col]
            prev_index, prev_house_code, prev_bumper_in, prev_bumper_out = None, '', '', ''

            for index, row_data in day_data.items():
                row_str = str(row_data).upper().strip()
                
                # Standard Rule: Check for Broken Glass unless explicitly ignored.
                if 'BROKEN GLASS' in row_str or 'STUNT' in row_str:
                    continue

                main_matches = re.findall(house_code_pattern, row_str)
                
                # Standard Rule: Process Media Lists unless explicitly ignored.
                media_list_matches = None
                qt_media_list_matches = None
                if not self.config.get('ignore_media_list_rule', False):
                    media_list_matches = re.search(media_list_pattern, row_str)
                    qt_media_list_matches = re.search(qt_media_list_pattern, row_str)
                
                current_house_code, bumper_in, bumper_out = None, '', ''

                if media_list_matches:
                    media_list_id = next(g for g in media_list_matches.groups() if g is not None)
                    current_house_code = f'MEDIALIST{media_list_id}'
                elif qt_media_list_matches:
                    current_house_code = f'MEDIALIST{qt_media_list_matches.group(1)}'
                elif main_matches:
                    current_house_code = '|ad_break|'.join(main_matches)
                    if bumper_pattern:
                        house_codes_found = list(re.finditer(house_code_pattern, row_str))
                        bumpers_found = list(re.finditer(bumper_pattern, row_str))
                        if house_codes_found and bumpers_found:
                            first_pos = house_codes_found[0].start()
                            bumper_in = '|ad_break|'.join([b.group(0) for b in bumpers_found if b.start() < first_pos])
                            bumper_out = '|ad_break|'.join([b.group(0) for b in bumpers_found if b.start() > first_pos])

                if current_house_code:
                    if prev_house_code and prev_index is not None:
                        duration = (index - prev_index) * 30
                        start_time = grid_data.at[prev_index, 'Start Time']
                        results.append({'House Code': prev_house_code, 'Bumper In': prev_bumper_in, 'Bumper Out': prev_bumper_out, 'Duration (minutes)': duration, 'Air Date': col, 'Start Time': start_time})
                    prev_house_code, prev_bumper_in, prev_bumper_out, prev_index = current_house_code, bumper_in, bumper_out, index
            
            if prev_house_code and prev_index is not None:
                duration = (len(day_data) - prev_index) * 30
                start_time = grid_data.at[prev_index, 'Start Time']
                results.append({'House Code': prev_house_code, 'Bumper In': prev_bumper_in, 'Bumper Out': prev_bumper_out, 'Duration (minutes)': duration, 'Air Date': col, 'Start Time': start_time})
        return pd.DataFrame(results)
    
    def _process_show_programming_pll_domestic(self, grid_data):
        #self.log("-> Applying PLL Domestic parsing rules.")
        
        results = []
        house_code_pattern = self.config['house_code_pattern']
        bumper_pattern = self.config.get('bumper_pattern')
        qt_media_list_pattern = r'QT\s+MEDIA\s?LIST[:\s]*?(\d+)'

        for col in grid_data.columns[1:8]:
            day_data = grid_data[col]
            prev_index, prev_house_code, prev_bumper_in, prev_bumper_out = None, '', '', ''

            for index, row_data in day_data.items():
                row_str = str(row_data).upper().strip()
                
                # Standard Rule: Check for Broken Glass unless explicitly ignored.
                if 'STUNT' in row_str:
                    continue

                main_matches = re.findall(house_code_pattern, row_str)
                
                # Standard Rule: Process Media Lists unless explicitly ignored.
                qt_media_list_matches = None
                qt_media_list_matches = re.search(qt_media_list_pattern, row_str)
                
                current_house_code, bumper_in, bumper_out = None, '', ''

                if qt_media_list_matches:
                    current_house_code = f'MEDIALIST{qt_media_list_matches.group(1)}'
                elif main_matches:
                    current_house_code = '|ad_break|'.join(main_matches)
                    if bumper_pattern:
                        house_codes_found = list(re.finditer(house_code_pattern, row_str))
                        bumpers_found = list(re.finditer(bumper_pattern, row_str))
                        if house_codes_found and bumpers_found:
                            first_pos = house_codes_found[0].start()
                            bumper_in = '|ad_break|'.join([b.group(0) for b in bumpers_found if b.start() < first_pos])
                            bumper_out = '|ad_break|'.join([b.group(0) for b in bumpers_found if b.start() > first_pos])

                if current_house_code:
                    if prev_house_code and prev_index is not None:
                        duration = (index - prev_index) * 30
                        start_time = grid_data.at[prev_index, 'Start Time']
                        results.append({'House Code': prev_house_code, 'Bumper In': prev_bumper_in, 'Bumper Out': prev_bumper_out, 'Duration (minutes)': duration, 'Air Date': col, 'Start Time': start_time})
                    prev_house_code, prev_bumper_in, prev_bumper_out, prev_index = current_house_code, bumper_in, bumper_out, index
            
            if prev_house_code and prev_index is not None:
                duration = (len(day_data) - prev_index) * 30
                start_time = grid_data.at[prev_index, 'Start Time']
                results.append({'House Code': prev_house_code, 'Bumper In': prev_bumper_in, 'Bumper Out': prev_bumper_out, 'Duration (minutes)': duration, 'Air Date': col, 'Start Time': start_time})
        return pd.DataFrame(results)
    
    def _process_show_programming_pll_domestic(self, grid_data):
        #self.log("-> Applying PLL Domestic-specific parsing rules.")
        
        results = []
        house_code_pattern = self.config['house_code_pattern']
        bumper_pattern = self.config.get('bumper_pattern')
        qt_media_list_pattern = r'QT\s+MEDIA\s?LIST[:\s]*?(\d+)'

        for col_name in grid_data.columns[1:8]:
            prev_index, prev_house_code, prev_bumpers_in, prev_bumpers_out = None, '', '', ''

            for index, row in grid_data.iterrows():
                cell_content = str(row[col_name]).upper().strip()
                
                # Rule: Check for Broken Glass unless the config flag is True.
                if 'STUNT' in cell_content:
                    continue

                is_new_block, current_house_code_str, current_bumpers_in_str, current_bumpers_out_str = False, '', '', ''

                if cell_content:
                    # Rule: Check for Media Lists unless the config flag is True.
                    qt_matches = None
                    qt_matches = re.search(qt_media_list_pattern, cell_content)

                    parts = [p.strip() for p in cell_content.split(',') if p.strip()]

                    if qt_matches:
                        is_new_block = True
                        current_house_code_str = f'MEDIALIST{qt_matches.group(1)}'
                    elif parts and any(re.match(house_code_pattern, p) or (bumper_pattern and re.match(bumper_pattern, p)) for p in parts):
                        is_new_block = True
                        house_codes, bumpers_in, bumpers_out = [], [], []
                        found_main_content = False
                        for part in parts:
                            if re.match(house_code_pattern, part):
                                house_codes.append(part)
                                found_main_content = True
                            elif bumper_pattern and re.match(bumper_pattern, part):
                                (bumpers_out if found_main_content else bumpers_in).append(part)
                        
                        current_house_code_str = '|ad_break|'.join(house_codes)
                        current_bumpers_in_str = '|ad_break|'.join(bumpers_in)
                        current_bumpers_out_str = '|ad_break|'.join(bumpers_out)

                if is_new_block:
                    if prev_index is not None:
                        duration = (index - prev_index) * 30
                        if duration > 0:
                            start_time = grid_data.at[prev_index, 'Start Time']
                            results.append({'House Code': prev_house_code, 'Bumper In': prev_bumpers_in, 'Bumper Out': prev_bumpers_out, 'Duration (minutes)': duration, 'Air Date': col_name, 'Start Time': start_time})
                    
                    prev_index, prev_house_code, prev_bumpers_in, prev_bumpers_out = index, current_house_code_str, current_bumpers_in_str, current_bumpers_out_str
            
            if prev_index is not None:
                duration = (len(grid_data) - prev_index) * 30
                if duration > 0:
                    start_time = grid_data.at[prev_index, 'Start Time']
                    results.append({'House Code': prev_house_code, 'Bumper In': prev_bumpers_in, 'Bumper Out': prev_bumpers_out, 'Duration (minutes)': duration, 'Air Date': col_name, 'Start Time': start_time})
        
        return pd.DataFrame(results)
    
    def _map_to_ids(self, house_code_str, library_sheet_df):
        house_codes = house_code_str.split('|ad_break|')
        mapped_ids = []
        for house_code in house_codes:
            if house_code.startswith('MEDIALIST'):
                mapped_ids.append(house_code.replace('MEDIALIST', ''))
            else:
                match = library_sheet_df[library_sheet_df['legacy_id'] == house_code]['id']
                if not match.empty:
                    mapped_ids.append(str(match.iloc[0]))
                elif house_code:
                    if house_code not in self.unmatched_ids:
                         self.unmatched_ids.append(house_code)
                    mapped_ids.append('')
        return '|ad_break|'.join(mapped_ids)
    
    def _create_final_sheet(self, programming_df, library_df):
        if programming_df.empty:
            self.log("WARNING: No programming blocks were found in the grid. Halting process.")
            return None

        self.log("Running validations...")
        unfit_durations = self._validate_slot_durations(programming_df, library_df)
        zero_duration_content = self._check_zero_duration_content(programming_df, library_df)

        self.unmatched_ids = []
        self.premature_mpls = []

        mapped_ids = programming_df['House Code'].apply(lambda x: self._map_to_ids(x, library_df))
        mapped_bumpers_in = programming_df['Bumper In'].apply(lambda x: self._map_to_ids(x, library_df))
        mapped_bumpers_out = programming_df['Bumper Out'].apply(lambda x: self._map_to_ids(x, library_df))

        # --- MODIFICATION START ---
        # The 'has_errors' flag will now only be set for critical errors.
        has_critical_errors = False

        if unfit_durations:
            # Duration mismatches are now treated as warnings and will be logged,
            # but will NOT halt the process.
            self.log("\n--- WARNING: DURATION MISMATCHES FOUND ---")
            for unfit in unfit_durations:
                content_duration_formatted = self._convert_seconds_to_hhmm(unfit['Content Duration (seconds)'])
                slot_duration_formatted = self._convert_seconds_to_hhmm(unfit['Slot Duration (minutes)'] * 60)
                valid_range_start, valid_range_end = unfit['Valid Range']
                self.log(
                    f"{unfit['House Code']} on {unfit['Air Date']} at {unfit['Start Time']}:\n"
                    f"  > Content duration ({content_duration_formatted}) is outside the valid range for a {slot_duration_formatted} slot.\n"
                    f"  > The valid duration range for this slot is between {valid_range_start} and {valid_range_end}."
                )

        if zero_duration_content:
            has_critical_errors = True
            self.log("\n--- CRITICAL ERROR: ZERO DURATION CONTENT DETECTED ---")
            for content in zero_duration_content: self.log(f"{content['House Code']} (ID: {content['Mapped IDs']}) needs reindexing.")
        
        if self.unmatched_ids:
            has_critical_errors = True
            self.log("\n--- CRITICAL ERROR: UNMATCHED HOUSE CODES (Not in library) ---")
            self.log(', '.join(sorted(list(set(self.unmatched_ids)))))
        
        if self.premature_mpls:
            self.log("\n--- MANUAL SCHEDULING MAY BE REQUIRED ---")
            self.log("The following MPLS codes were found. Please verify them in OTTera:")
            self.log(', '.join(sorted(list(set(self.premature_mpls)))))
        
        # The process will only halt if there are CRITICAL errors.
        if has_critical_errors:
             return None
        # --- MODIFICATION END ---
             
        self.log("All critical validations passed. Assembling final sheet...")
        output_df = pd.DataFrame()
        output_df['date'] = programming_df['Air Date']
        output_df['linear_channel'] = self.config['linear_channel_id']
        output_df['bumpers_in'] = mapped_bumpers_in.apply(lambda x: str(x).split('.')[0])
        output_df['bumpers_in'] = output_df['bumpers_in'].str.replace('|ad_break', '', regex=False)
        output_df['bumpers_out'] = mapped_bumpers_out.apply(lambda x: str(x).split('.')[0])
        output_df['bumpers_out'] = output_df['bumpers_out'].str.replace('|ad_break', '', regex=False)
        output_df['content'] = mapped_ids.astype(str).apply(lambda x: str(x).split('.')[0])
        
        output_df['randomize_content'] = 'FALSE'
        output_df['slot_duration'] = programming_df['Duration (minutes)']
        output_df['time_slot'] = programming_df['Start Time']
        
        promo_in = self.config.get('hourly_promo_in')
        promo_out = self.config.get('hourly_promo_out')
        if promo_in and promo_out:
            self.log("Applying hourly promos...")
            output_df['hour'] = output_df['time_slot'].str[:2]
            output_df['is_new_hour'] = output_df['hour'] != output_df['hour'].shift()
            output_df.loc[output_df['is_new_hour'], 'content'] = (promo_in + "|" + output_df.loc[output_df['is_new_hour'], 'content'].astype(str) + "|" + promo_out)
            output_df = output_df.drop(columns=['hour', 'is_new_hour'])


        output_df['content'] += '|ad_break'
        
        final_columns = ['date', 'linear_channel', 'bumpers_in', 'bumpers_out', 'content', 'randomize_content', 'slot_duration', 'time_slot']
        
        return output_df[final_columns]

    def _convert_seconds_to_hhmm(self, seconds):
        if pd.isna(seconds): return "00:00"
        total_minutes = int(seconds) // 60
        return f"{total_minutes // 60:02}:{total_minutes % 60:02}"

    def _is_valid_duration(self, slot_duration, content_duration_seconds):
        content_duration_hhmm = self._convert_seconds_to_hhmm(content_duration_seconds)
        valid_durations = {
            30: ("00:00", "00:25"), 60: ("00:26", "00:55"), 90: ("00:51", "01:15"),
            120: ("01:16", "01:40"), 150: ("01:41", "02:05"), 180: ("02:06", "02:30"),
            210: ("02:31", "02:55"), 240: ("02:56", "03:20"), 270: ("03:21", "03:45"),
            300: ("03:46", "04:10"), 330: ("04:11", "04:35"), 360: ("04:36", "05:00")
        }
        if slot_duration in valid_durations:
            start_time, end_time = valid_durations[slot_duration]
            return start_time <= content_duration_hhmm <= end_time
        return False
    
    def _validate_slot_durations(self, programming_df, library_df):
        unfit = []
        merged = programming_df.merge(library_df[['legacy_id', 'duration']], left_on='House Code', right_on='legacy_id', how='left')
        for _, row in merged.iterrows():
            if pd.notna(row['duration']) and '|ad_break|' not in str(row['House Code']) and 'MEDIALIST' not in str(row['House Code']):
                content_duration_hhmm = self._convert_seconds_to_hhmm(row['duration'])
                valid_durations = {30: ("00:00", "00:25"), 60: ("00:26", "00:55"), 90: ("00:51", "01:15"), 120: ("01:16", "01:40"), 150: ("01:41", "02:05"), 180: ("02:06", "02:30"), 210: ("02:31", "02:55"), 240: ("02:56", "03:20"), 270: ("03:21", "03:45"), 300: ("03:46", "04:10"), 330: ("04:11", "04:35"), 360: ("04:36", "05:00")}
                slot_duration = row['Duration (minutes)']
                if slot_duration in valid_durations:
                    start_time, end_time = valid_durations[slot_duration]
                    if not (start_time <= content_duration_hhmm <= end_time) and row['duration'] != 0:
                        # --- THIS IS THE MODIFIED PART ---
                        # Add the valid range to the dictionary we return.
                        unfit.append({
                            'House Code': row['House Code'], 
                            'Slot Duration (minutes)': slot_duration, 
                            'Content Duration (seconds)': int(row['duration']), 
                            'Air Date': row['Air Date'], 
                            'Start Time': row['Start Time'],
                            'Valid Range': (start_time, end_time) # <-- ADD THIS LINE
                        })
                        # --- END OF MODIFICATION ---
        return unfit
    
    def _check_zero_duration_content(self, programming_df, library_df):
        zero = []
        merged = programming_df.merge(library_df[['legacy_id', 'id', 'duration']], left_on='House Code', right_on='legacy_id', how='left')
        zero_duration_content = merged[merged['duration'] == 0]
        for _, row in zero_duration_content.iterrows():
            zero.append({'House Code': row['House Code'], 'Mapped IDs': str(row['id']).split('.')[0]})
        return zero

@app.command("/create-schedule")
def handle_generation_command(ack, body, client):
    """This function is triggered when a user runs the slash command."""
    # Acknowledge the command immediately
    ack()
    
    try:
        # Open the modal for user input
        client.views_open(
            trigger_id=body["trigger_id"],
            view={
                "type": "modal",
                "callback_id": "generate_schedule_modal",
                "title": {"type": "plain_text", "text": "Schedule Generator"},
                "submit": {"type": "plain_text", "text": "Generate"},
                "close": {"type": "plain_text", "text": "Cancel"},
                "blocks": [
                    {
                        "type": "context",
                        "elements": [
                            {
                                "type": "mrkdwn",
                                "text": "ⓘ *Important*: Please make sure you have uploaded your library CSV file to me *before* running this command."
                            }
                        ]
                    },
                    {
                        "type": "input",
                        "block_id": "channel_block",
                        "label": {"type": "plain_text", "text": "1. Select Channel"},
                        "element": {
                            "type": "static_select",
                            "action_id": "channel_select",
                            "placeholder": {"type": "plain_text", "text": "Select a channel..."},
                            "options": [
                                {"text": {"type": "plain_text", "text": name}, "value": name}
                                for name in CHANNEL_CONFIG.keys()
                            ]
                        }
                    },
                    {
                        "type": "input",
                        "block_id": "date_block",
                        "label": {"type": "plain_text", "text": "2. Select Schedule Date"},
                        "element": {
                            "type": "datepicker",
                            "action_id": "date_select",
                            "initial_date": datetime.now().strftime('%Y-%m-%d'),
                            "placeholder": {"type": "plain_text", "text": "Select a date"}
                        }
                    }
                ]
            }
        )
    except Exception as e:
        print(f"Error opening modal: {e}")

def run_processing_in_thread(config, date_str, library_content, client, channel_id, thread_ts):
    """Wrapper to run the engine in a separate thread."""
    engine = ProcessingEngine(config, date_str, library_content, client, channel_id, thread_ts)
    engine.run()

# --- MODIFIED: This function handles the modal submission ---
@app.view("generate_schedule_modal")
def handle_modal_submission(ack, body, client, view):
    """This function handles the submission of the modal."""
    ack()

    user_id = body["user"]["id"]
    values = view["state"]["values"]
    selected_channel = values["channel_block"]["channel_select"]["selected_option"]["value"]
    selected_date = values["date_block"]["date_select"]["selected_date"]
    
    try:
        # --- ADDED: Convert User ID to DM Channel ID ---
        # This is necessary because files.upload needs the 'D...' channel ID, not the 'U...' user ID.
        dm_channel_response = client.conversations_open(users=user_id)
        dm_channel_id = dm_channel_response["channel"]["id"]
        # --- END OF ADDITION ---

        initial_msg = client.chat_postMessage(
            channel=dm_channel_id, # Use the new DM channel ID
            text=f"Getting *{selected_date}* schedule for *{selected_channel}*..."
        )
        thread_ts = initial_msg["ts"]

        # Find the most recent CSV uploaded by the user
        client.chat_postMessage(channel=dm_channel_id, thread_ts=thread_ts, text="Searching for your most recent CSV file...")
        
        files_response = client.files_list(user=user_id, filetype="csv", count=1)
        if not files_response["files"]:
            client.chat_postMessage(channel=dm_channel_id, thread_ts=thread_ts, text="❌ Error: I couldn't find any CSV files you've uploaded. Please upload the library CSV and try again.")
            return
            
        latest_file = files_response["files"][0]
        file_id = latest_file["id"]
        client.chat_postMessage(channel=dm_channel_id, thread_ts=thread_ts, text=f"Using library sheet: `{latest_file['name']}`. Processing grid...")

        # Download the library CSV content from Slack
        response = requests.get(
            latest_file["url_private_download"],
            headers={"Authorization": f"Bearer {os.environ['SLACK_BOT_TOKEN']}"}
        )
        response.raise_for_status()
        library_content = response.text

        # Start the long-running process in a background thread
        config = CHANNEL_CONFIG[selected_channel]
        thread = threading.Thread(
            target=run_processing_in_thread,
            args=(config, selected_date, library_content, client, dm_channel_id, thread_ts) # Pass the new DM channel ID
        )
        thread.start()

    except Exception as e:
        # Also post errors to the user's DM
        error_dm_channel_id = client.conversations_open(users=user_id)["channel"]["id"]
        client.chat_postMessage(
            channel=error_dm_channel_id,
            text=f"Sorry, a critical error occurred: `{e}`"
        )


if __name__ == "__main__":
    print("🤖 Slack bot is running...")
    # Use SocketModeHandler for easy local development
    SocketModeHandler(app, os.environ["SLACK_APP_TOKEN"]).start()