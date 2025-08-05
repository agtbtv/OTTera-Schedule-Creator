import re
import os
import sys
import requests
import pandas as pd
from datetime import datetime, timedelta

# --- Helper Functions ---

def get_week_name_of_input_date(input_date_str):
    """
    Parses an input date string and generates a week name (e.g., "Aug 5-Aug 11").
    Returns the week name and the parsed datetime object.
    """
    input_date_formats = ["%m%d%Y", "%m/%d/%Y", "%m-%d-%Y", "%m%d%y", "%m/%d/%y", "%m-%d-%y"]
    parsed_date = None
    for date_format in input_date_formats:
        try:
            parsed_date = datetime.strptime(input_date_str, date_format)
            break
        except ValueError:
            continue
    
    if parsed_date is None:
        raise ValueError("Invalid input date format. Please use a standard format like MM/DD/YYYY.")

    start_of_week = parsed_date - timedelta(days=parsed_date.weekday())
    end_of_week = start_of_week + timedelta(days=6)

    week_start_month = start_of_week.strftime("%b")
    week_end_month = end_of_week.strftime("%b")
    week_name = f"{week_start_month} {start_of_week.day}-{week_end_month} {end_of_week.day}"
    
    return week_name, parsed_date

def map_to_ids(code_str, library_sheet_df):
    """Maps a string of house codes or bumper codes to their corresponding OTTera node IDs."""
    if not isinstance(code_str, str) or not code_str:
        return ''
        
    codes = code_str.split('|ad_break|')
    mapped_ids = []
    for code in codes:
        if code.startswith('MEDIALIST'):
            media_list_id = code.split('MEDIALIST')[1]
            mapped_ids.append(media_list_id)
        else:
            normalized_code = re.sub(r'(\D+)\s+(\d+)', r'\1\2', code)
            match = library_sheet_df[library_sheet_df['legacy_id'] == normalized_code]['id']
            if not match.empty:
                if 'MPLS' in normalized_code:
                    premature_mpls.append(normalized_code)
                mapped_ids.append(str(match.iloc[0]))
            elif code:
                unmatched_ids.append(normalized_code)
                mapped_ids.append('')
    return '|ad_break|'.join(mapped_ids)

def convert_seconds_to_hhmm(seconds):
    """Converts a duration in seconds to a HH:MM formatted string."""
    if pd.isna(seconds):
        return "00:00"
    total_minutes = int(seconds) // 60
    hours = total_minutes // 60
    minutes = total_minutes % 60
    return f"{int(hours):02}:{int(minutes):02}"

# --- Core Processing Functions ---

def parse_programming_grid(grid_data):
    """
    Parses the schedule grid to identify programming blocks and their start times.
    Duration is NOT calculated here.
    """
    results = []
    
    house_code_pattern = r'(MPLS_EP\s?\d+|PLL\s?\d+|PLLH\s?\d+|PLLFILL\s?\d+|MPLS\s?\d+)'
    bumper_pattern = r'(PLLBUMP\s?\d+)'
    qt_media_list_pattern = r'QT\s+MEDIA\s?LIST[:\s]*?(\d+)'
    all_codes_pattern = r'(?:QT\s+MEDIA\s?LIST[:\s]*?\d+|MPLS_EP\s?\d+|PLL\s?\d+|PLLH\s?\d+|PLLFILL\s?\d+|MPLS\s?\d+|PLLBUMP\s?\d+)'

    def _parse_cell_state(cell_content_str):
        normalized_content = cell_content_str.upper().replace('\n', ' ').strip()
        found_parts = [re.sub(r'(\D+)\s+(\d+)', r'\1\2', m) for m in re.findall(all_codes_pattern, normalized_content)]
        
        house_codes, bumpers_in, bumpers_out = [], [], []
        found_house_code_in_cell = False

        for part in found_parts:
            qt_media_match = re.search(qt_media_list_pattern, part)
            is_house_code = re.fullmatch(house_code_pattern, part)
            is_bumper = re.fullmatch(bumper_pattern, part)

            if is_house_code or qt_media_match:
                house_codes.append(f'MEDIALIST{qt_media_match.group(1)}' if qt_media_match else part)
                found_house_code_in_cell = True
            elif is_bumper:
                (bumpers_out if found_house_code_in_cell else bumpers_in).append(part)
        
        return ('|ad_break|'.join(house_codes), '|ad_break|'.join(bumpers_in), '|ad_break|'.join(bumpers_out))

    for col_name in grid_data.columns[1:8]:
        if grid_data.empty:
            continue
            
        last_state = _parse_cell_state(str(grid_data.iloc[0][col_name]))
        last_block_start_index = 0

        for index in range(1, len(grid_data) + 1):
            current_state = None
            if index < len(grid_data):
                current_state = _parse_cell_state(str(grid_data.iloc[index][col_name]))

            if current_state != last_state:
                house_code, bumpers_in, bumpers_out = last_state
                if house_code or bumpers_in or bumpers_out:
                    start_time = grid_data.iloc[last_block_start_index, 0]
                    results.append({
                        'House Code': house_code,
                        'Bumpers In': bumpers_in,
                        'Bumpers Out': bumpers_out,
                        'Air Date': col_name,
                        'Start Time': start_time,
                    })
                
                last_state = current_state
                last_block_start_index = index

    return pd.DataFrame(results)

def filter_unique_rows_by_latest_date(library_sheet_filepath):
    """Reads the library CSV and keeps only the most recent entry for each legacy_id."""
    try:
        df = pd.read_csv(library_sheet_filepath, low_memory=False)
        df_sorted = df.sort_values(by=['legacy_id', 'id'], ascending=[True, False])
        unique_df = df_sorted.drop_duplicates(subset=['legacy_id'], keep='first')
        return unique_df
    except FileNotFoundError:
        print(f"❌ Error: The library file was not found at '{library_sheet_filepath}'.")
        return pd.DataFrame()
    except KeyError as e:
        print(f"❌ Error: A required column is missing in the library file: {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"❌ An unexpected error occurred while reading the library file: {e}")
        return pd.DataFrame()

# --- Validation Functions ---

def is_valid_duration(slot_duration, content_duration_seconds):
    """Checks if a video's duration is valid for its scheduled time slot."""
    content_duration_hhmm = convert_seconds_to_hhmm(content_duration_seconds)
    
    # Defines the acceptable runtime range for each slot length
    valid_durations = {
        30: ("00:00", "00:25"), 60: ("00:26", "00:55"), 90: ("00:51", "01:15"),
        120: ("01:16", "01:40"), 150: ("01:41", "02:05"), 180: ("02:06", "02:30"),
        210: ("02:31", "02:55"), 240: ("02:56", "03:20"), 270: ("03:21", "03:45"),
        300: ("03:46", "04:10"), 330: ("04:11", "04:35"), 360: ("04:36", "05:00")
    }
    
    if slot_duration in valid_durations:
        start_time, end_time = valid_durations[slot_duration]
        is_valid = start_time <= content_duration_hhmm <= end_time
        return is_valid, (start_time, end_time)
    
    return False, None

def validate_slot_durations(programming_df, library_df):
    """Validates that the scheduled slot duration is appropriate for the content's actual duration."""
    unfit_durations = []

    for _, row in programming_df.iterrows():
        house_code = row['House Code']
        if '|ad_break|' in house_code or 'MEDIALIST' in house_code or not house_code:
            continue

        library_row = library_df[library_df['legacy_id'] == house_code]
        if not library_row.empty:
            slot_duration_min = row['Duration (minutes)']
            content_duration_sec = library_row.iloc[0]['duration']
            
            is_valid, valid_range = is_valid_duration(slot_duration_min, content_duration_sec)
            if not is_valid:
                start_range, end_range = valid_range if valid_range else ("N/A", "N/A")
                unfit_durations.append({
                    'House Code': house_code,
                    'Slot Duration (min)': slot_duration_min,
                    'Content Duration (sec)': content_duration_sec,
                    'Air Date': row['Air Date'],
                    'Start Time': row['Start Time'],
                    'Reminder': f"between {start_range} and {end_range}"
                })
    return unfit_durations

def check_zero_duration_content(programming_df, library_df):
    """Identifies any scheduled content that has a duration of 0 in the library."""
    zero_duration_items = []
    for _, row in programming_df.iterrows():
        house_code = row['House Code']
        if '|ad_break|' in house_code or not house_code:
            continue
        
        library_row = library_df[library_df['legacy_id'] == house_code]
        if not library_row.empty:
            content_duration = library_row.iloc[0]['duration']
            if pd.isna(content_duration) or content_duration == 0:
                mapped_id = map_to_ids(house_code, library_df)
                zero_duration_items.append({'House Code': house_code, 'Mapped ID': mapped_id})
    
    if zero_duration_items:
        print("\n🚨 ERROR: The following content has a duration of 0 and needs to be re-indexed:")
        unique_content = {item['House Code']: item['Mapped ID'] for item in zero_duration_items}
        for code, mapped_id in sorted(unique_content.items()):
            print(f"- {code} (OTTera node ID: {mapped_id})")
        sys.exit(1)

# --- Main Execution ---
if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python your_script_name.py <date> <path_to_library_sheet.csv>")
        sys.exit(1)

    input_date_str = sys.argv[1]
    library_sheet_filepath = sys.argv[2]
    
    home_dir = os.path.expanduser("~")
    downloads_folder = os.path.join(home_dir, "Downloads")

    try:
        week_name, input_date_obj = get_week_name_of_input_date(input_date_str)
    except ValueError as e:
        print(f"❌ Error: {e}")
        sys.exit(1)

    # 1. Download schedule from Google Sheets
    spreadsheet_id = '1qLC9nSmQHB7pd8lIEe6NXyQzs_49mSnWv53I4cq6EcQ'
    sheet_name = week_name.upper()
    temp_download_file = os.path.join(downloads_folder, f'temp_PLL_Domestic_Grid_{sheet_name}.csv')
    url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"

    print(f"\n⚙️  Processing Domestic schedule for week: {sheet_name}...")
    print("Downloading grid from Google Sheets...")
    response = requests.get(url)
    if response.status_code == 200:
        with open(temp_download_file, 'wb') as f:
            f.write(response.content)
        print("✅ Grid downloaded successfully.")
    else:
        print(f"❌ Failed to download grid. Status code: {response.status_code}. Sheet '{sheet_name}' may not exist.")
        sys.exit(1)

    # 2. Clean and prepare the downloaded data
    grid_data = pd.read_csv(temp_download_file)
    grid_data = grid_data.iloc[:50].copy()
    grid_data = grid_data.drop(grid_data.columns[-1], axis=1)
    grid_data = grid_data.drop(grid_data.index[:2]).reset_index(drop=True)

    week_start_date = input_date_obj - timedelta(days=input_date_obj.weekday())
    week_dates = [(week_start_date + timedelta(days=i)).strftime("%m/%d/%Y") for i in range(7)]
    grid_data.columns = ['Start Time'] + week_dates

    grid_data['Start Time'] = pd.to_datetime(grid_data['Start Time'], format='%I:%M %p', errors='coerce').dt.strftime('%H:%M')
    grid_data = grid_data.fillna('')

    # 3. Parse grid for programming blocks
    print("Parsing programming grid...")
    library_sheet_df = filter_unique_rows_by_latest_date(library_sheet_filepath)
    if library_sheet_df.empty:
        sys.exit(1)
        
    programming_df = parse_programming_grid(grid_data)

    # 4. Calculate slot durations based on start times
    if not programming_df.empty:
        programming_df['start_datetime'] = pd.to_datetime(
            programming_df['Air Date'] + ' ' + programming_df['Start Time'],
            format='%m/%d/%Y %H:%M'
        )
        programming_df['next_start_datetime'] = programming_df.groupby('Air Date')['start_datetime'].shift(-1)
        last_slots_mask = programming_df['next_start_datetime'].isna()
        end_of_day_datetimes = programming_df.loc[last_slots_mask, 'start_datetime'].dt.normalize() + pd.Timedelta(days=1)
        programming_df.loc[last_slots_mask, 'next_start_datetime'] = end_of_day_datetimes
        duration = (programming_df['next_start_datetime'] - programming_df['start_datetime']).dt.total_seconds() / 60
        programming_df['Duration (minutes)'] = duration.astype(int)
        programming_df.drop(columns=['start_datetime', 'next_start_datetime'], inplace=True)

    # 5. Validate data and map codes
    unmatched_ids = []
    premature_mpls = []
    check_zero_duration_content(programming_df, library_sheet_df)
    unfit_durations = validate_slot_durations(programming_df, library_sheet_df)
    if unfit_durations:
        print("\n⚠️  WARNING: Duration mismatches found!")
        for unfit in unfit_durations:
            print(f"  - On {unfit['Air Date']} at {datetime.strptime(unfit['Start Time'], '%H:%M').strftime('%-I:%M%p').lower()}, "
                  f"'{unfit['House Code']}' (duration: {convert_seconds_to_hhmm(unfit['Content Duration (sec)'])}) "
                  f"is in a {unfit['Slot Duration (min)']} min slot. "
                  f"Valid range is {unfit['Reminder']}.")

    # 6. Create Final Output DataFrame
    print("Mapping codes to OTTera IDs...")
    output_df = pd.DataFrame()
    output_df['date'] = programming_df['Air Date']
    output_df['slot_duration'] = programming_df['Duration (minutes)']
    output_df['time_slot'] = programming_df['Start Time']

    output_df['content'] = programming_df['House Code'].apply(lambda x: map_to_ids(x, library_sheet_df))
    output_df['bumpers_in'] = programming_df['Bumpers In'].apply(lambda x: map_to_ids(x, library_sheet_df))
    output_df['bumpers_in'] = output_df['bumpers_in'].str.replace('|ad_break', '', regex=False)
    output_df['bumpers_out'] = programming_df['Bumpers Out'].apply(lambda x: map_to_ids(x, library_sheet_df))
    output_df['bumpers_out'] = output_df['bumpers_out'].str.replace('|ad_break', '', regex=False)
    
    output_df['linear_channel'] = 9 # Domestic Channel ID
    output_df['randomize_content'] = 'FALSE'
    
    # Add hourly promos
    output_df['hour'] = output_df['time_slot'].str[:2]
    is_new_hour = output_df['hour'] != output_df['hour'].shift()
    output_df.loc[is_new_hour, 'content'] = "6139|" + output_df.loc[is_new_hour, 'content'].astype(str) + "|6336"
    
    # Add ad break
    output_df['content'] = output_df['content'].astype(str).str.cat(['|ad_break'] * len(output_df))
    
    # 7. Finalize and Export
    final_columns = [
        'date', 'linear_channel', 'slot_duration', 'time_slot', 
        'content', 'randomize_content', 'bumpers_in', 'bumpers_out'
    ]
    output_df = output_df.drop(columns=['hour'])[final_columns]
    
    output_filename = os.path.join(downloads_folder, f'PLL_Domestic_Schedule_Sheet_{sheet_name}.csv')
    output_df.to_csv(output_filename, index=False)

    print(f"\n✅ Success! Schedule sheet saved to:\n{output_filename}")
    
    # 8. Final Report
    if premature_mpls:
        print('\n❗ NOTICE: Manual Scheduling Required')
        print('The following MPLS codes were found. Please schedule them MANUALLY in OTTera:')
        for mpls in sorted(list(set(premature_mpls))):
            print(f"- {mpls}")

    if unmatched_ids:
        print('\n❌ ERROR: Unmatched Content Found')
        print("The following codes were in the schedule but NOT in the OTTera library:")
        unique_unmatched_ids = sorted(list(set(unmatched_ids)))
        print(', '.join(unique_unmatched_ids))
        print("Please correct the schedule or update the library and run the script again.")

    os.remove(temp_download_file)