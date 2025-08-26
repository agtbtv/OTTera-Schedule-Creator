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
    #"Bowling": {
    #    "spreadsheet_id": '1yj4FjX1uv3irJbftSGTP-xCdZhtovoP1f3RCdRk-cek',
    #    "linear_channel_id": 0,
    #    "house_code_pattern": r'(BOWL\d+|BOWLFILL\d+)',
    #    "bumper_pattern": r'(BOWLBUMP\d+)',
    #    "output_prefix": "BowlingTV",
    #    "processing_logic": "standard"
    #},
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
    "SLVR": {
        "spreadsheet_id": '1Vi6vr5lI41SM9yV4y0HVeq0tMreJmhMp4s1coVKPTHw',
        "linear_channel_id": 7260,
        "house_code_pattern": r'(BOX\d+|EGH\d+|SLVR\d+|BOXFILL\d+|EGHFILL\d+|SLVRFILL\d+)',
        "bumper_pattern": r'(BOXBUMP\d+|EGHBUMP\d+|SLVRBUMP\d+)',
        "output_prefix": "SLVR",
        "processing_logic": "slvr"
    },
    "SLVR SoCal": {
        "spreadsheet_id": '1Vi6vr5lI41SM9yV4y0HVeq0tMreJmhMp4s1coVKPTHw',
        "linear_channel_id": 7790,
        "house_code_pattern": r'(BOX\d+|EGH\d+|SLVR\d+|BOXFILL\d+|EGHFILL\d+|SLVRFILL\d+)',
        "bumper_pattern": r'(BOXBUMP\d+|EGHBUMP\d+|SLVRBUMP\d+)',
        "output_prefix": "SLVR_SOCAL",
        "processing_logic": "slvr socal"
    },

}

class ProcessingEngine:
    def __init__(self, config, input_date_str, library_file_content):
        self.config = config
        self.input_date_str = input_date_str
        self.library_file_content = library_file_content
        self.logs = [] # A new list to store log messages
        self.unmatched_ids = []
        self.premature_mpls = []

    # --- MODIFIED: The log() method now appends to the internal list ---
    def log(self, message):
        """Adds a log message to an internal list instead of posting to Slack."""
        self.logs.append(message)

    # --- REVISION 1: The run() method now RETURNS the DataFrame instead of uploading it. ---
    def run(self):
        try:
            week_name, input_date = self._get_week_name_of_input_date(self.input_date_str)
            if not week_name: return None

            downloads_folder = str(Path.home() / "Downloads")
            temp_grid_file = os.path.join(downloads_folder, f"temp_grid_{self.config['output_prefix']}.csv")
            if not self._download_sheet(self.config['spreadsheet_id'], week_name.upper(), temp_grid_file): return None

            grid_data = self._prepare_grid_data(temp_grid_file, input_date)
            
            if self.config.get('processing_logic') == 'pll domestic':
                programming_df = self._process_show_programming_pll_domestic(grid_data)
            elif self.config.get('processing_logic') == 'slvr':
                programming_df = self._process_show_programming_slvr(grid_data)
            elif self.config.get('processing_logic') == 'slvr socal':
                programming_df = self._process_show_programming_slvr_socal(grid_data)
            else:
                programming_df = self._process_show_programming_standard(grid_data)
            
            self.log("Getting OTTera node IDs...")
            library_df = self._filter_unique_rows_by_latest_date(self.library_file_content)
            if library_df.empty: return None

            final_df = self._create_final_sheet(programming_df, library_df)

            # If the process failed, log it and return None
            if final_df is None:
                self.log(f"--- {self.config['output_prefix']} | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---")
                self.log("🚫 *PROCESS HALTED* for this channel due to validation errors found above.")
                return None
            
            # On success, return the created DataFrame
            return final_df

        except Exception as e:
            self.log(f"\n--- A CRITICAL ERROR OCCURRED for {self.config['output_prefix']} ---\n`{e}`")
            import traceback
            self.log(f"```\n{traceback.format_exc()}\n```")
            return None # Ensure we return None on a critical error
        finally:
            if 'temp_grid_file' in locals() and os.path.exists(temp_grid_file):
                os.remove(temp_grid_file)
    
    # --- The rest of your ProcessingEngine methods remain unchanged ---
    def _filter_unique_rows_by_latest_date(self, csv_content):
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
        url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"
        try:
            response = requests.get(url, timeout=30)
            if response.status_code == 200:
                with open(output_file, 'wb') as f:
                    f.write(response.content)
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

    def _run_validations(self, programming_df, library_df):
        """A dedicated method to run all checks and log the results."""
        self.log("Running validations...")
        unfit_durations = self._validate_slot_durations(programming_df, library_df)
        zero_duration_content = self._check_zero_duration_content(programming_df, library_df)

        # We must call _map_to_ids to populate self.unmatched_ids
        self.unmatched_ids = []
        # We can map all unique House Codes at once for efficiency
        all_house_codes = pd.unique(programming_df[['House Code', 'Bumper In', 'Bumper Out']].values.ravel('K'))
        all_house_codes_str = '|ad_break|'.join(filter(None, all_house_codes))
        self._map_to_ids(all_house_codes_str, library_df)

        has_critical_errors = False

        if unfit_durations:
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
        
        return has_critical_errors
    
    def _run_validations(self, programming_df, library_df):
        """
        A dedicated method to run all checks and log the results with unique lists
        for relevant errors.
        """
        self.log("Running validations...")
        # Call the helper methods you provided to get the raw error lists
        unfit_durations = self._validate_slot_durations(programming_df, library_df)
        zero_duration_content = self._check_zero_duration_content(programming_df, library_df)

        # We must call _map_to_ids to populate self.unmatched_ids
        self.unmatched_ids = []
        # Gather all unique house codes from the schedule to check them at once
        all_house_codes = pd.unique(programming_df[['House Code', 'Bumper In', 'Bumper Out']].values.ravel('K'))
        all_house_codes_str = '|ad_break|'.join(filter(None, all_house_codes))
        self._map_to_ids(all_house_codes_str, library_df)

        has_critical_errors = False

        # For duration mismatches, each instance is reported with its unique date/time context.
        if unfit_durations:
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

        # --- REVISED: This section now reports a unique list of zero-duration codes ---
        if zero_duration_content:
            has_critical_errors = True
            self.log("\n--- CRITICAL ERROR: ZERO DURATION CONTENT DETECTED ---")
            # Use a dictionary to store unique house codes and an example mapped ID.
            # This automatically handles duplicates.
            unique_zero_duration = {
                content['House Code']: content['Mapped IDs'] 
                for content in zero_duration_content
            }
            # Format the unique codes into a single, clean string for the log.
            error_list = sorted([f"{code} (ID: {uid})" for code, uid in unique_zero_duration.items()])
            self.log("The following house codes need reindexing: \n" + '\n'.join(error_list))
        
        # Reports a unique list of any house codes that were not found in the library.
        if self.unmatched_ids:
            has_critical_errors = True
            self.log("\n--- CRITICAL ERROR: UNMATCHED HOUSE CODES (Not in library) ---")
            # The 'set' automatically removes all duplicates from the list.
            unique_unmatched = sorted(list(set(self.unmatched_ids)))
            self.log("The following house codes were not found: \n" + '\n'.join(unique_unmatched))
        
        # Reports a unique list of MPLS codes, if any were found.
        if self.premature_mpls:
            self.log("\n--- MANUAL SCHEDULING MAY BE REQUIRED ---")
            unique_mpls = sorted(list(set(self.premature_mpls)))
            self.log("The following MPLS codes were found and should be verified: \n" + '\n'.join(unique_mpls))
        
        return has_critical_errors
    
    def validate_only(self):
        """
        Runs the entire process up to the validation step and reports the results
        without generating a final CSV.
        """
        try:
            # Perform all the same initial steps as the run() method
            week_name, input_date = self._get_week_name_of_input_date(self.input_date_str)
            if not week_name: return False

            downloads_folder = str(Path.home() / "Downloads")
            temp_grid_file = os.path.join(downloads_folder, f"temp_grid_{self.config['output_prefix']}.csv")
            if not self._download_sheet(self.config['spreadsheet_id'], week_name.upper(), temp_grid_file): return False

            grid_data = self._prepare_grid_data(temp_grid_file, input_date)
            
            if self.config.get('processing_logic') == 'pll domestic':
                programming_df = self._process_show_programming_pll_domestic(grid_data)
            elif self.config.get('processing_logic') == 'slvr':
                programming_df = self._process_show_programming_slvr(grid_data)
            elif self.config.get('processing_logic') == 'slvr_socal':
                programming_df = self._process_show_programming_slvr_socal(grid_data)
            else:
                programming_df = self._process_show_programming_standard(grid_data)
            
            library_df = self._filter_unique_rows_by_latest_date(self.library_file_content)
            if library_df.empty: return False

            # Run the validations and report the outcome
            has_critical_errors = self._run_validations(programming_df, library_df)

            if has_critical_errors:
                self.log("\n🚫 Validation Failed.")
            else:
                self.log("\n✅ All validations passed successfully!")
            
            return not has_critical_errors

        except Exception as e:
            self.log(f"\n--- A CRITICAL ERROR OCCURRED during validation for {self.config['output_prefix']} ---\n`{e}`")
            import traceback
            self.log(f"```\n{traceback.format_exc()}\n```")
            return False
        finally:
            if 'temp_grid_file' in locals() and os.path.exists(temp_grid_file):
                os.remove(temp_grid_file)

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
                
                # --- THIS IS THE FIX ---
                # The line that checked for "BROKEN GLASS" or "STUNT" and skipped the
                # entire cell has been removed. The logic now correctly prioritizes
                # finding a media list first.
                # ----------------------

                main_matches = re.findall(house_code_pattern, row_str)
                
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
        results = []
        house_code_pattern = self.config['house_code_pattern']
        bumper_pattern = self.config.get('bumper_pattern')
        qt_media_list_pattern = r'QT\s+MEDIA\s?LIST[:\s]*?(\d+)'

        broken_glass_pattern = re.compile(r'BROKEN\s?GLASS:?\s*(' + house_code_pattern + r')')

        for col_name in grid_data.columns[1:8]:
            prev_index, prev_house_code, prev_bumpers_in, prev_bumpers_out = None, '', '', ''

            for index, row in grid_data.iterrows():
                cell_content = str(row[col_name]).upper().strip()
                is_new_block, current_house_code_str, current_bumpers_in_str, current_bumpers_out_str = False, '', '', ''

                if cell_content:
                    # --- THIS IS THE FIX ---
                    # Replace newline characters with commas to handle multi-line cells.
                    cell_content = cell_content.replace('\n', ',')
                    # --- END OF FIX ---

                    qt_matches = re.search(qt_media_list_pattern, cell_content)
                    parts = [p.strip() for p in cell_content.split(',') if p.strip()]

                    if qt_matches:
                        is_new_block = True
                        current_house_code_str = f'MEDIALIST{qt_matches.group(1)}'
                    
                    elif parts and any(
                        broken_glass_pattern.match(p) or
                        re.match(house_code_pattern, p) or
                        (bumper_pattern and re.match(bumper_pattern, p))
                        for p in parts
                    ):
                        is_new_block = True
                        house_codes, bumpers_in, bumpers_out = [], [], []
                        found_main_content = False
                        for part in parts:
                            bg_match = broken_glass_pattern.match(part)

                            if bg_match:
                                house_codes.append(bg_match.group(1))
                                found_main_content = True
                            elif re.match(house_code_pattern, part):
                                house_codes.append(part)
                                found_main_content = True
                            elif bumper_pattern and re.match(bumper_pattern, part):
                                (bumpers_out if found_main_content else bumpers_in).append(part)
                        
                        current_house_code_str = '|ad_break|'.join(house_codes)
                        current_bumpers_in_str = '|ad_break|'.join(bumpers_in)
                        current_bumpers_out_str = '|ad_break|'.join(bumpers_out)

                if is_new_block and current_house_code_str:
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
    
    def _process_show_programming_slvr(self, grid_data):
        """
        Processes the grid for SLVR. It uses standard logic but adds a special
        rule to schedule 'BROKEN GLASS' only when 'SOCAL MEDIA LIST' is also present.
        """
        results = []
        house_code_pattern = self.config['house_code_pattern']
        bumper_pattern = self.config.get('bumper_pattern')
        # Standard pattern for "MEDIA LIST" and "ML"
        media_list_pattern = r'^MEDIA\s?LIST[:\s]*?(\d+)|[^\w\s][\s]*MEDIA\s?LIST[:\s]*?(\d+)|^ML[:\s]*?(\d+)|[^\w\s][\s]*ML[:\s]*?(\d+)'
        # A simple pattern to check for the presence of a SoCal media list
        socal_check_pattern = r'SOCAL\s+(MEDIA\s?LIST|ML)'

        for col in grid_data.columns[1:8]:
            day_data = grid_data[col]
            prev_index, prev_house_code, prev_bumper_in, prev_bumper_out = None, '', '', ''

            for index, row_data in day_data.items():
                row_str = str(row_data).upper().strip()
                current_house_code, bumper_in, bumper_out = None, '', ''

                media_list_matches = re.search(media_list_pattern, row_str)
                main_matches = re.findall(house_code_pattern, row_str)

                # Highest priority: Check for the special SLVR rule
                if re.search(socal_check_pattern, row_str) and 'BROKEN GLASS' in row_str:
                    current_house_code = 'BROKEN GLASS'
                # Next priority: Check for standard "MEDIA LIST" or "ML"
                elif media_list_matches:
                    media_list_id = next(g for g in media_list_matches.groups() if g is not None)
                    current_house_code = f'MEDIALIST{media_list_id}'
                # Fallback to regular house codes
                elif main_matches:
                    current_house_code = '|ad_break|'.join(main_matches)
                    # Standard bumper logic
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

    def _process_show_programming_slvr_socal(self, grid_data):
        """
        Processes the grid for SLVR SoCal. It uses standard logic but adds
        recognition for 'SOCAL MEDIA LIST' and 'SOCAL ML'.
        """
        results = []
        house_code_pattern = self.config['house_code_pattern']
        bumper_pattern = self.config.get('bumper_pattern')
        # Standard pattern for "MEDIA LIST" and "ML"
        media_list_pattern = r'^MEDIA\s?LIST[:\s]*?(\d+)|[^\w\s][\s]*MEDIA\s?LIST[:\s]*?(\d+)|^ML[:\s]*?(\d+)|[^\w\s][\s]*ML[:\s]*?(\d+)'
        # New pattern specifically for SoCal media lists
        socal_media_list_pattern = r'SOCAL\s+MEDIA\s?LIST[:\s]*?(\d+)|SOCAL\s+ML[:\s]*?(\d+)'

        for col in grid_data.columns[1:8]:
            day_data = grid_data[col]
            prev_index, prev_house_code, prev_bumper_in, prev_bumper_out = None, '', '', ''

            for index, row_data in day_data.items():
                row_str = str(row_data).upper().strip()
                current_house_code, bumper_in, bumper_out = None, '', ''

                # Check for all media list variations
                socal_media_list_matches = re.search(socal_media_list_pattern, row_str)
                media_list_matches = re.search(media_list_pattern, row_str)
                main_matches = re.findall(house_code_pattern, row_str)

                # Highest priority: SoCal Media Lists
                if socal_media_list_matches:
                    media_list_id = next(g for g in socal_media_list_matches.groups() if g is not None)
                    current_house_code = f'MEDIALIST{media_list_id}'
                # Next priority: Standard Media Lists
                elif media_list_matches:
                    media_list_id = next(g for g in media_list_matches.groups() if g is not None)
                    current_house_code = f'MEDIALIST{media_list_id}'
                # Fallback to regular house codes
                elif main_matches:
                    current_house_code = '|ad_break|'.join(main_matches)
                    # Standard bumper logic
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
        has_critical_errors = False
        if unfit_durations:
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
        if has_critical_errors:
             return None
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
    
    def _create_final_sheet(self, programming_df, library_df):
        if programming_df.empty:
            self.log("WARNING: No programming blocks were found in the grid. Halting process.")
            return None

        has_critical_errors = self._run_validations(programming_df, library_df)
        if has_critical_errors:
            return None # Halt the process if critical errors are found
              
        self.log("All critical validations passed. Assembling final sheet...")
        
        mapped_ids = programming_df['House Code'].apply(lambda x: self._map_to_ids(x, library_df))
        mapped_bumpers_in = programming_df['Bumper In'].apply(lambda x: self._map_to_ids(x, library_df))
        mapped_bumpers_out = programming_df['Bumper Out'].apply(lambda x: self._map_to_ids(x, library_df))

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
                        unfit.append({
                            'House Code': row['House Code'], 
                            'Slot Duration (minutes)': slot_duration, 
                            'Content Duration (seconds)': int(row['duration']), 
                            'Air Date': row['Air Date'], 
                            'Start Time': row['Start Time'],
                            'Valid Range': (start_time, end_time)
                        })
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
    ack()
    try:
        client.views_open(
            trigger_id=body["trigger_id"],
            view={"type": "modal","callback_id": "generate_schedule_modal","title": {"type": "plain_text", "text": "Schedule Generator"},"submit": {"type": "plain_text", "text": "Generate"},"close": {"type": "plain_text", "text": "Cancel"},"blocks": [{"type": "context","elements": [{"type": "mrkdwn","text": "ⓘ *Important*: Please make sure you have uploaded your library CSV file to me *before* running this command."}]},{"type": "input","block_id": "channel_block","label": {"type": "plain_text", "text": "1. Select Channels"},"element": {"type": "checkboxes","action_id": "channel_checkboxes","options": [{"text": {"type": "plain_text", "text": name}, "value": name} for name in CHANNEL_CONFIG.keys()]}},{"type": "actions","elements": [{"type": "button","text": {"type": "plain_text","text": "Select All","emoji": True},"action_id": "select_all_channels_action"}]},{"type": "input","block_id": "date_block","label": {"type": "plain_text", "text": "2. Select Schedule Date"},"element": {"type": "datepicker","action_id": "date_select","initial_date": datetime.now().strftime('%Y-%m-%d'),"placeholder": {"type": "plain_text", "text": "Select a date"}}}]}
        )
    except Exception as e:
        print(f"Error opening modal: {e}")

@app.command("/validate-schedule")
def handle_validation_command(ack, body, client):
    """This function is triggered when a user runs the /validate-schedule command."""
    ack()
    
    try:
        # Open a modal for user input, similar to the generation command
        client.views_open(
            trigger_id=body["trigger_id"],
            view={
                "type": "modal",
                "callback_id": "validate_schedule_modal", # Different callback_id
                "title": {"type": "plain_text", "text": "Schedule Validator"},
                "submit": {"type": "plain_text", "text": "Validate"},
                "close": {"type": "plain_text", "text": "Cancel"},
                # The blocks are identical to the other modal
                "blocks": [{"type": "context","elements": [{"type": "mrkdwn","text": "ⓘ *Important*: Please make sure you have uploaded your library CSV file to me *before* running this command."}]},{"type": "input","block_id": "channel_block","label": {"type": "plain_text", "text": "1. Select Channels"},"element": {"type": "checkboxes","action_id": "channel_checkboxes","options": [{"text": {"type": "plain_text", "text": name}, "value": name} for name in CHANNEL_CONFIG.keys()]}},{"type": "actions","elements": [{"type": "button","text": {"type": "plain_text","text": "Select All","emoji": True},"action_id": "select_all_channels_action"}]},{"type": "input","block_id": "date_block","label": {"type": "plain_text", "text": "2. Select Schedule Date"},"element": {"type": "datepicker","action_id": "date_select","initial_date": datetime.now().strftime('%Y-%m-%d'),"placeholder": {"type": "plain_text", "text": "Select a date"}}}]}
        )
    except Exception as e:
        print(f"Error opening validation modal: {e}")

@app.action("select_all_channels_action")
def handle_select_all_channels(ack, body, client):
    """Handles the 'Select All' button click in the modal."""
    ack()
    view = body['view']
    view_id = view['id']
    all_channel_options = [{"text": {"type": "plain_text", "text": name}, "value": name} for name in CHANNEL_CONFIG.keys()]
    for block in view['blocks']:
        if block.get('block_id') == 'channel_block':
            block['element']['initial_options'] = all_channel_options
            break
    updated_view = {"type": "modal","callback_id": view["callback_id"],"title": view["title"],"submit": view["submit"],"close": view["close"],"blocks": view["blocks"]}
    try:
        client.views_update(view_id=view_id, view=updated_view)
    except Exception as e:
        print(f"Error updating view: {e}")

def process_channel_and_store_result(config, date_str, library_content, client, channel_id, thread_ts, results_list):
    """
    Runs the processing for one channel, posts a single summary message with all logs,
    and appends the resulting DataFrame to a shared list.
    """
    # The engine no longer takes Slack client details directly
    engine = ProcessingEngine(config, date_str, library_content)
    result_df = engine.run()  # This runs the processing and collects logs internally

    final_status_message = ""
    log_summary = "\n".join(engine.logs) # Combine all collected logs

    if result_df is not None and not result_df.empty:
        results_list.append(result_df)
        final_status_message = (
            f"✅ *{config['output_prefix']}* - Success\n"
            f"```{log_summary}```"
        )
    else:
        final_status_message = (
            f"⚠️ *{config['output_prefix']}* - Failed\n"
            f"```{log_summary}```"
        )
    
    client.chat_postMessage(
        channel=channel_id,
        thread_ts=thread_ts,
        text=final_status_message
    )

def validate_channel_and_report(config, date_str, library_content, client, channel_id, thread_ts, success_list):
    """
    Runs the validation-only process for one channel and posts the summary log.
    """
    engine = ProcessingEngine(config, date_str, library_content)
    # The validate_only() method runs the checks and collects logs internally
    was_successful = engine.validate_only() 

    # Combine all collected logs into one message
    log_summary = "\n".join(engine.logs)
    
    # Append the success status to a shared list for a final summary
    success_list.append(was_successful)
    
    client.chat_postMessage(
        channel=channel_id,
        thread_ts=thread_ts,
        text=f"--- Validation Results for *{config['output_prefix']}* ---\n```{log_summary}```"
    )

@app.view("validate_schedule_modal")
def handle_validation_modal_submission(ack, body, client, view):
    """
    Handles the validation modal, runs the validation process for each channel,
    and posts a final summary.
    """
    user_id = body["user"]["id"]
    values = view["state"]["values"]
    selected_options = values["channel_block"]["channel_checkboxes"]["selected_options"]
    selected_channels = [opt["value"] for opt in selected_options]
    selected_date = values["date_block"]["date_select"]["selected_date"]

    if not selected_channels:
        ack(response_action="errors", errors={"channel_block": "Please select at least one channel."})
        return
    ack()
    
    try:
        # This logic is very similar to the main submission handler
        dm_channel_response = client.conversations_open(users=user_id)
        dm_channel_id = dm_channel_response["channel"]["id"]

        files_response = client.files_list(user=user_id, filetype="csv", count=1)
        if not files_response["files"]:
            client.chat_postMessage(channel=dm_channel_id, text="❌ Error: I couldn't find any CSV files you've uploaded. Please upload the library CSV and try again.")
            return
        
        latest_file = files_response["files"][0]
        response = requests.get(
            latest_file["url_private_download"],
            headers={"Authorization": f"Bearer {os.environ['SLACK_BOT_TOKEN']}"}
        )
        response.raise_for_status()
        library_content = response.text

        initial_msg = client.chat_postMessage(
            channel=dm_channel_id,
            text=f"🕵️‍♀️ Validation request received!\n• Using library: `{latest_file['name']}`\n• Validating schedules for: *{', '.join(selected_channels)}*."
        )
        thread_ts = initial_msg["ts"]

        validation_success_list = [] # A list to track the outcome of each channel
        threads = []
        for channel_name in selected_channels:
            config = CHANNEL_CONFIG[channel_name]
            thread = threading.Thread(
                target=validate_channel_and_report,
                args=(config, selected_date, library_content, client, dm_channel_id, thread_ts, validation_success_list)
            )
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()

        # Post a final summary message
        if all(validation_success_list):
            final_summary = "✅ *Overall Result:* All selected schedules passed validation."
        else:
            final_summary = "🚫 *Overall Result:* One or more schedules failed validation. Please review the details above."
        
        client.chat_postMessage(
            channel=dm_channel_id,
            thread_ts=thread_ts,
            text=final_summary
        )

    except Exception as e:
        error_dm_channel_id = client.conversations_open(users=user_id)["channel"]["id"]
        client.chat_postMessage(
            channel=error_dm_channel_id,
            text=f"Sorry, a critical error occurred during the validation process: `{e}`"
        )

@app.view("generate_schedule_modal")
def handle_modal_submission(ack, body, client, view):
    """
    Handles modal submission, runs processing for each channel,
    and combines the results into a single CSV file with a dynamic name.
    """
    user_id = body["user"]["id"]
    values = view["state"]["values"]
    selected_options = values["channel_block"]["channel_checkboxes"]["selected_options"]
    selected_channels = [opt["value"] for opt in selected_options]
    selected_date = values["date_block"]["date_select"]["selected_date"]

    if not selected_channels:
        ack(response_action="errors", errors={"channel_block": "Please select at least one channel."})
        return
    ack()
    
    try:
        dm_channel_response = client.conversations_open(users=user_id)
        dm_channel_id = dm_channel_response["channel"]["id"]

        # --- MODIFIED: Consolidate the startup logic before sending the first message ---
        # First, find and download the library file content silently.
        files_response = client.files_list(user=user_id, filetype="csv", count=1)
        if not files_response["files"]:
            client.chat_postMessage(channel=dm_channel_id, text="❌ Error: I couldn't find any CSV files you've uploaded. Please upload the library CSV and try again.")
            return
        
        latest_file = files_response["files"][0]
        response = requests.get(
            latest_file["url_private_download"],
            headers={"Authorization": f"Bearer {os.environ['SLACK_BOT_TOKEN']}"}
        )
        response.raise_for_status()
        library_content = response.text

        # Now, post a single, consolidated startup message.
        initial_msg = client.chat_postMessage(
            channel=dm_channel_id,
            text=f"🚀 Request received!\n• Using library: `{latest_file['name']}`\n• Generating a combined schedule for *{', '.join(selected_channels)}* for the week of *{selected_date}*."        )
        thread_ts = initial_msg["ts"]
        # --- End of Modification ---

        # Create and start a thread for each channel.
        results_dataframes = []
        threads = []
        for channel_name in selected_channels:
            config = CHANNEL_CONFIG[channel_name]
            thread = threading.Thread(
                target=process_channel_and_store_result,
                # Note: We now pass the Slack client details here for the summary message
                args=(config, selected_date, library_content, client, dm_channel_id, thread_ts, results_dataframes)
            )
            thread.start()
            threads.append(thread)

        # Wait for all threads to complete their work.
        for thread in threads:
            thread.join()

        # Check if any results were successful, then combine and upload.
        if not results_dataframes:
            client.chat_postMessage(
                channel=dm_channel_id,
                thread_ts=thread_ts,
                text="_All channels failed to process. No combined schedule sheet was created._"
            )
            return

        client.chat_postMessage(channel=dm_channel_id, thread_ts=thread_ts, text="Combining all successful schedules...")
        
        master_df = pd.concat(results_dataframes, ignore_index=True)
        # This is the new sorting logic
        master_df.sort_values(by=['linear_channel', 'date', 'time_slot'], inplace=True)
        
        prefixes = sorted([CHANNEL_CONFIG[ch]['output_prefix'] for ch in selected_channels])
        filename_prefix = "_".join(prefixes)
        output_filename = f"{filename_prefix}_Schedule_Sheet_{selected_date}.csv"
        
        client.files_upload_v2(
            channel=dm_channel_id,
            thread_ts=thread_ts,
            content=master_df.to_csv(index=False),
            filename=output_filename,
            initial_comment="🎉 Here is your combined schedule!"
        )

    except Exception as e:
        error_dm_channel_id = client.conversations_open(users=user_id)["channel"]["id"]
        client.chat_postMessage(
            channel=error_dm_channel_id,
            text=f"Sorry, a critical error occurred during the main process: `{e}`"
        )


if __name__ == "__main__":
    print("🤖 Slack bot is running...")
    SocketModeHandler(app, os.environ["SLACK_APP_TOKEN"]).start()