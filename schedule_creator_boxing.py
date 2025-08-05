import re
import os
import sys
import requests
import pandas as pd
from datetime import datetime, timedelta

def get_week_name_of_input_date(input_date_str):
    # Normalize input date to handle various formats
    input_date_formats = ["%m%d%Y", "%m/%d/%Y", "%m-%d-%Y", "%m%d%y", "%m/%d/%y", "%m-%d-%y"]
    for date_format in input_date_formats:
        try:
            input_date = datetime.strptime(input_date_str, date_format)
            break
        except ValueError:
            continue
    else:
        raise ValueError("Input date format is invalid. Please use MMDDYY, MM/DD/YY, MM-DD-YY, MMDDYYYY, MM/DD/YYYY, or MM-DD-YYYY.")

    # Find the Monday of the week
    start_of_week = input_date - timedelta(days=input_date.weekday())  # Monday of that week
    end_of_week = start_of_week + timedelta(days=6)  # Sunday of that week

    # Format the week name with month abbreviations
    week_start_month = start_of_week.strftime("%b")
    week_end_month = end_of_week.strftime("%b")

    week_name = f"{week_start_month} {start_of_week.day}-{week_end_month} {end_of_week.day}"


    return week_name, input_date

def get_next_monday_after_input_date(input_date_str):
    # Normalize input date to handle various formats
    input_date_formats = ["%m%d%Y", "%m/%d/%Y", "%m-%d-%Y", "%m%d%y", "%m/%d/%y", "%m-%d-%y"]
    for date_format in input_date_formats:
        try:
            input_date = datetime.strptime(input_date_str, date_format)
            break
        except ValueError:
            continue
    else:
        raise ValueError("Input date format is invalid. Please use MMDDYY, MM/DD/YY, MM-DD-YY, MMDDYYYY, MM/DD/YYYY, or MM-DD-YYYY.")

    # Find the Monday of the current week
    start_of_week = input_date - timedelta(days=input_date.weekday())  # Monday of that week

    # Get the next Monday (one week later)
    next_monday = start_of_week + timedelta(weeks=1)

    return next_monday.strftime("%m/%d/%Y")

def process_show_programming(grid_data):
    results = []
    pattern = r'(BOX\d+|BOXFILL\d+)'
    media_list_pattern = r'^MEDIA\s?LIST[:\s]*?(\d+)|[^\w\s][\s]*MEDIA\s?LIST[:\s]*?(\d+)|^ML[:\s]*?(\d+)|[^\w\s][\s]*ML[:\s]*?(\d+)'
    qt_media_list_pattern = r'QT\s+MEDIA\s?LIST[:\s]*?(\d+)'
    bumper_pattern = r'(25PREDEUROCHAMP_BUMP|BOXB\d+)'

    for col in grid_data.columns[:8]:
        day_data = grid_data[col].fillna('')
        prev_index = None
        prev_house_code = ''
        prev_row = ''

        for i, (index, row_data) in enumerate(day_data.items()):
            matches = re.findall(pattern, str(row_data).upper().strip())
            media_list_matches = re.search(media_list_pattern, str(row_data).upper())
            qt_media_list_matches = re.search(qt_media_list_pattern, str(row_data).upper())

            # Extract the number from 'MEDIA LIST: [number]'
            if media_list_matches:
                media_list_id = media_list_matches.group(1)

                # Append the previous house code before overwriting it with the media list
                if prev_house_code is not None and prev_index is not None:
                    duration = (index - prev_index) * 30
                    start_time = grid_data.iloc[prev_index, 0]
                    end_time = (datetime.strptime(start_time, "%H:%M") + timedelta(minutes=duration)).strftime("%H:%M")

                    results.append({
                        'House Code': prev_house_code,
                        'Duration (minutes)': duration,
                        'Air Date': col,
                        'Start Time': start_time,
                        'End Time': end_time
                    })

                # Now update prev_house_code to the media list
                prev_house_code = f'MEDIALIST{media_list_id}'
                prev_index = index  # Set index for the current MEDIA LIST
                
            elif qt_media_list_matches:
                media_list_id = qt_media_list_matches.group(1)

                # Append the previous house code before overwriting it with the media list
                if prev_house_code is not None and prev_index is not None:
                    duration = (index - prev_index) * 30
                    start_time = grid_data.iloc[prev_index, 0]
                    end_time = (datetime.strptime(start_time, "%H:%M") + timedelta(minutes=duration)).strftime("%H:%M")

                    results.append({
                        'House Code': prev_house_code,
                        'Duration (minutes)': duration,
                        'Air Date': col,
                        'Start Time': start_time,
                        'End Time': end_time
                    })

                # Now update prev_house_code to the media list
                prev_house_code = f'MEDIALIST{media_list_id}'
                prev_index = index  # Set index for the current MEDIA LIST

            elif matches and 'BROKEN GLASS' not in row_data.upper() and 'STUNT' not in row_data.upper():
                house_code = '|ad_break|'.join(matches)

                # Append the previous house code if applicable
                if prev_house_code is not None and prev_index is not None:
                    duration = (index - prev_index) * 30
                    start_time = grid_data.iloc[prev_index, 0]
                    end_time = (datetime.strptime(start_time, "%H:%M") + timedelta(minutes=duration)).strftime("%H:%M")

                    results.append({
                        'House Code': prev_house_code,
                        'Duration (minutes)': duration,
                        'Air Date': col,
                        'Start Time': start_time,
                        'End Time': end_time
                    })

                # Update to the new house code
                prev_house_code = house_code
                prev_index = index

        if prev_index is not None:
            duration = (day_data.index[-1] - prev_index + 1) * 30
            start_time = grid_data.iloc[prev_index, 0]
            end_time = (datetime.strptime(start_time, "%H:%M") + timedelta(minutes=duration)).strftime("%H:%M")
            results.append({
                'House Code': prev_house_code,
                'Duration (minutes)': duration,
                'Air Date': col,
                'Start Time': start_time,
                'End Time': end_time
            })

    expiration_date = get_next_monday_after_input_date(input_date_str)
    grid_data_results = pd.DataFrame(results)
    grid_data_results['Expiration Date'] = expiration_date
    return grid_data_results

def map_to_ids(house_code_str, library_sheet_df):

    # Split the house codes if multiple are delimited by '|ad_break|'
    house_codes = house_code_str.split('|ad_break|')
    # Map each house code to the corresponding id from the library_sheet_df
    mapped_ids = []
    for house_code in house_codes:
        # Check if house_code is from 'MEDIA LIST: [number]'
        if house_code.startswith('MEDIALIST'):
            media_list_id = house_code.split('MEDIALIST')[1]
            mapped_ids.append(media_list_id)  # Use the extracted MEDIA LIST ID directly
        else:
            # Find the matching id for each house code
            match = library_sheet_df[library_sheet_df['legacy_id'] == house_code]['id']
            if not match.empty:
                mapped_ids.append(str(match.iloc[0]))  # Append the matched id as a string
            elif house_code != '':
                unmatched_ids.append(house_code)  # Add unmatched house code to the list
                mapped_ids.append('')  # Append an empty string if no match is found
    # Join the mapped ids back together with '|ad_break|'
    return '|ad_break|'.join(mapped_ids)

def convert_seconds_to_hhmm(seconds):
    """Convert seconds to HH:MM format"""
    total_minutes = seconds // 60
    hours = total_minutes // 60
    minutes = total_minutes % 60
    return f"{int(hours):02}:{int(minutes):02}"

def is_valid_duration(slot_duration, content_duration_seconds):
    """Check if the content duration fits the slot duration"""
    content_duration_hhmm = convert_seconds_to_hhmm(content_duration_seconds)

    # Define the valid duration ranges for each slot duration (in HH:MM format)
    valid_durations = {
        30: ("00:00", "00:25"),
        60: ("00:26", "00:55"),
        90: ("00:51", "01:15"),
        120: ("01:16", "01:40"),
        150: ("01:41", "02:05"),
        180: ("02:06", "02:30"),
        210: ("02:31", "02:55"),
        240: ("02:56", "03:20"),
        270: ("03:21", "03:45"),
        300: ("03:46", "04:10"),
        330: ("04:11", "04:35"),
        360: ("04:36", "05:00")
    }

    if slot_duration in valid_durations:
        start_time, end_time = valid_durations[slot_duration]
        is_valid = start_time <= content_duration_hhmm <= end_time
        return is_valid, (start_time, end_time)
    else:
        # If the slot duration is not in the predefined valid ranges
        return False, None

# Function to validate each slot in the programming grid against the library sheet durations
def validate_slot_durations(programming_grid_df, library_sheet_df):
    unfit_durations = []  # Store any rows with unfit durations
    for index, row in programming_grid_df.iterrows():
        slot_duration_minutes = row['Duration (minutes)']
        house_code = row['House Code']

        # Find the corresponding content duration in the library sheet
        library_row = library_sheet_df[library_sheet_df['legacy_id'] == house_code]
        if not library_row.empty:
            content_duration_seconds = library_row.iloc[0]['duration']

            # Validate the duration against the slot duration
            is_valid, valid_range = is_valid_duration(slot_duration_minutes, content_duration_seconds)
            if not is_valid:
                start_time, end_time = valid_range if valid_range else ("N/A", "N/A")
                unfit_durations.append({
                    'House Code': house_code,
                    'Slot Duration (minutes)': slot_duration_minutes,
                    'Content Duration (seconds)': int(content_duration_seconds),
                    'Air Date': row['Air Date'],
                    'Start Time': row['Start Time'],
                    'Reminder': f"{start_time} and {end_time}."
                })

    return unfit_durations

def check_zero_duration_content(programming_grid_df, library_sheet_df):
    zero_duration_content = []

    for index, row in programming_grid_df.iterrows():
        house_code = row['House Code']
        
        # Find the corresponding content duration in the library sheet
        library_row = library_sheet_df[library_sheet_df['legacy_id'] == house_code]
        if not library_row.empty:
            content_duration_seconds = library_row.iloc[0]['duration']

            # Check if the content duration is 0
            if content_duration_seconds == 0 or pd.isna(content_duration_seconds):
                mapped_ids = map_to_ids(house_code, library_sheet_df)
                zero_duration_content.append({
                    'House Code': house_code,
                    'Mapped IDs': mapped_ids
                })
    
    if zero_duration_content:
        print("The following content needs to be reindexed in OTTera: ")
        unique_content = {content['House Code']: content['Mapped IDs'] for content in zero_duration_content}
    
        for house_code, mapped_ids in sorted(unique_content.items()):
            print(f"{house_code} (OTTera node ID: {mapped_ids})")
        sys.exit()

def filter_unique_rows_by_latest_date(library_sheet_filepath):
    legacy_id_col = 'legacy_id'
    id_col = 'id'

    try:
        # Read the CSV file into a pandas DataFrame
        df = pd.read_csv(library_sheet_filepath)
        
        # Sort the DataFrame - first by the legacy_id, then by storage_id in descending order (latest date first)
        df_sorted = df.sort_values(by=[legacy_id_col, id_col], ascending=[True, False])

        # Since we sorted node id in descending order, keep the 'first' occurrence, drop the latter ones.
        unique_df = df_sorted.drop_duplicates(subset=[legacy_id_col], keep='first')

        return unique_df

    except FileNotFoundError:
        print(f"Error: The file '{library_sheet_filepath}' was not found. Please check the path.")
        return pd.DataFrame() # Return an empty DataFrame on error
    except KeyError as e:
        print(f"Error: One of the specified columns was not found in the CSV. Missing column: {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return pd.DataFrame()
    
# Get the user's home directory
home_dir = os.path.expanduser("~")

# Construct the path to the Downloads folder
downloads_folder = os.path.join(home_dir, "Downloads")

# Get the input date string and library sheet path passed from the GUI
input_date_str = sys.argv[1]

# Convert input date string to a datetime object
try:
    input_date = datetime.strptime(input_date_str, "%m/%d/%Y")
except ValueError:
    print(f"Error: Invalid date format. Expected mm/dd/yyyy, but got {input_date_str}.")
    sys.exit(1)
if sys.argv[2]:
    library_sheet_filepath = sys.argv[2]
else:
    print("No OTTera library sheet selected.")
    sys.exit(1)
    
# Get date input and week name
week_name, input_date = get_week_name_of_input_date(input_date_str)

# Get week dates
week_dates = []
for i in range(7):
    date = input_date + timedelta(days=i)
    week_dates.append(date.strftime("%m/%d/%Y"))

# Add air time columns
week_dates.insert(0, 'Start Time')

spreadsheet_id = '1jdMKwExqP3g0KpmCTrbOdxS74eAHLaeIsZPb-00NgT0'
sheet_id = week_name.upper()
output_path = downloads_folder
output_file = f'{downloads_folder}/BoxingTVProgrammingSheet({sheet_id}).csv'

# Construct the export URL for the specific sheet/tab
url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_id}"

# Send a GET request to the URL
response = requests.get(url)

# Save the CSV content to a file
if response.status_code == 200:
    with open(output_file, 'wb') as f:
        f.write(response.content)
    print(f"\n{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | Creating Boxing TV schedule for {sheet_id}...")
    
    # Read csv
    full_grid_data = pd.read_csv(output_file)

    # drop the last few rows to keep only rows in 24hr grid
    grid_data = full_grid_data[:50]
    
    # drop the first and last columns (scheduled air times)
    grid_data = grid_data.drop(grid_data.columns[[8]], axis=1)

    # drop the first two rows
    grid_data = grid_data.drop(grid_data.index[:2]).reset_index(drop=True)
    
    # convert air time to proper format
    grid_data.columns = week_dates
    grid_data['Start Time'] = pd.to_datetime(grid_data['Start Time'])
    grid_data['Start Time'] = grid_data['Start Time'].dt.strftime('%H:%M')

    # Replace NaN values with an empty string to avoid issues with regex and other operations
    grid_data = grid_data.fillna('')

    active_house_codes_filename = 'BoxingTV_Active_House_Codes_' + sheet_id + '.csv'
    sheet_data_filename = 'BoxingTV_Schedule_Data_' + sheet_id  + '.csv'

    full_active_house_codes = process_show_programming(grid_data)
    #full_active_house_codes['House Code'] = full_active_house_codes['House Code'].str.upper()
    #full_active_house_codes['House Code'] = full_active_house_codes['House Code']
    #full_active_house_codes.to_csv(sheet_data_filename, index=False)

    #active_house_codes = full_active_house_codes[['House Code', 'Expiration Date']]
    #active_house_codes = active_house_codes.reset_index(drop=True)
    #active_house_codes.to_csv(active_house_codes_filename, index=False, header=False)
    #print(f'Active house codes saved to: {active_house_codes_filename}!')

    #library_sheet_filepath = "/Users/aarongarner/Downloads/2025-04-07-bbb-video-export.csv"

    # Load the programming_grid.csv and library_sheet.csv
    programming_grid_df = full_active_house_codes
    library_sheet_df = filter_unique_rows_by_latest_date(library_sheet_filepath)
    
    # Prepare the output DataFrame
    output_df = pd.DataFrame()
    
    # List to store unmatched house codes
    unmatched_ids = []

    # Map relevant columns from programming_grid.csv
    output_df['date'] = programming_grid_df['Air Date']
    output_df['slot_duration'] = programming_grid_df['Duration (minutes)']
    output_df['time_slot'] = programming_grid_df['Start Time']

    # Merge the two DataFrames to get the 'id' based on matching 'House Code' with 'legacy_id'
    merged_df = programming_grid_df.merge(library_sheet_df[['legacy_id', 'id']], 
                                        left_on='House Code', 
                                        right_on='legacy_id', 
                                        how='left')

    # Apply the mapping function to the 'House Code' column in the merged DataFrame
    merged_df['id'] = merged_df['House Code'].apply(lambda x: map_to_ids(x, library_sheet_df))

    # Validate the slot durations
    unfit_durations = validate_slot_durations(programming_grid_df, library_sheet_df)

    # Print any unfit durations found
    if unfit_durations:
        print("Duration misalignment:")
        for unfit in unfit_durations:
            if unfit['Content Duration (seconds)'] != 0:
                print(f"{unfit['House Code']}\n"
                f"Air date: {unfit['Air Date']}\n"
                f"Start time: {datetime.strptime(unfit['Start Time'], '%H:%M').strftime('%I:%M%p').lstrip('0').lower()}\n"
                f"Slot duration: {convert_seconds_to_hhmm(unfit['Slot Duration (minutes)'] * 60)}\n"
                f"Content duration: {convert_seconds_to_hhmm(unfit['Content Duration (seconds)'])}\n"
                f"**The valid duration range for a {convert_seconds_to_hhmm(unfit['Slot Duration (minutes)'] * 60)} program is between {unfit['Reminder']}\n")

    # Check for zero-duration content
    check_zero_duration_content(programming_grid_df, library_sheet_df)

    # Print unmatched house codes if any are found
    if unmatched_ids:
        unique_unmatched_ids = set(unmatched_ids)  # Get unique house codes
        print("Not in OTTera library: " + ', '.join(unique_unmatched_ids))
        sys.exit()

    #if unfit_durations or unmatched_ids:
    #    sys.exit()

    # Assign the 'id' to output_df
    output_df['id'] = merged_df['id']

    # Assign constant values for other columns
    output_df['linear_channel'] = 2797
    output_df['bumpers_1_bumpers'] = 'Array'
    output_df['content'] = output_df['id'] # 'content' is set to 'id'
    output_df['randomize_content'] = 'FALSE'

    # Remove everything after the '.' in the 'id' column using .loc and integer conversion
    output_df.loc[:, 'id'] = output_df['id'].apply(lambda x: str(x).split('.')[0])
    output_df.loc[:, 'content'] = output_df['content'].apply(lambda x: str(x).split('.')[0])

    # Add ad break after content
    output_df['content'] = output_df['content'].astype(str) + '|ad_break'

    # Create a new DataFrame to hold the blank rows with 'ad_break' in 'content' column # no need for this anymore
    #blank_row = pd.DataFrame({
    #    'id': [''] * len(output_df),
    #    'date': [''] * len(output_df),
    #    'linear_channel': [''] * len(output_df),
    #    'bumplers_1_bumpers': [''] * len(output_df),
    #    'content': ['ad_break'] * len(output_df),
    #    'randomize_content': [''] * len(output_df),
    #    'slot_duration': [''] * len(output_df),
    #    'time_slot': [''] * len(output_df)
    #})

    # Add blank rows between each original row
    #output_with_blanks = pd.concat([output_df, blank_row], ignore_index=True).sort_index(kind="merge") # no need for this anymore

    # Create an empty list to hold the interleaved rows
    #ad_breaks = []

    # Iterate through each row in output_df and append the row followed by a blank row
    #for _, row in output_df.iterrows():
    #    ad_breaks.append(row)  # Add the original row
    #    ad_breaks.append(blank_row.iloc[0])  # Add the 'ad_break' row

    # Convert the list of interleaved rows back into a DataFrame
    #output_with_blanks = pd.DataFrame(ad_breaks)

    #output_with_blanks = output_with_blanks[['id', 'date', 'linear_channel', 'bumpers_1_bumpers', 'content', 'randomize_content',	'slot_duration', 'time_slot']]
    output_df = output_df[['date', 'linear_channel', 'content', 'randomize_content', 'slot_duration', 'time_slot']]

    # Export the resulting DataFrame to a new CSV
    #output_with_blanks.to_csv('BoxingTV_Schedule_Sheet_' + sheet_id + '.csv', index=False)
    output_df.to_csv(f'{downloads_folder}/BoxingTV_Schedule_Sheet_{sheet_id}.csv', index=False)

    print(f'Schedule sheet saved to {downloads_folder}/BoxingTV_Schedule_Sheet_{sheet_id}.csv!"')
    os.remove(output_file)

else:
    print(f"\nCould not get Boxing TV {sheet_id} grid. Status code: {response.status_code}.")