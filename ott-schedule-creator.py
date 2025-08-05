import os
import sys
import subprocess
import tkinter as tk
from tkcalendar import Calendar
import tkinter.font as tkFont
from tkinter import filedialog

# Function to run the selected script with the input date
def run_script(script_name, input_date_str):
    try:
        #python_executable = "/Users/aarongarner/Desktop/OTT-SCHEDULE-CREATOR/ott_sched_env/bin/python3"  # For Linux/macOS
        python_executable = sys.executable
        command = [python_executable, os.path.join(os.path.dirname(__file__), script_name), input_date_str]
        result = subprocess.run(command, capture_output=True, text=True)
        
        output_text.insert(tk.END, result.stdout)  # Insert new output
        output_text.see(tk.END)  # Auto-scroll to the bottom

        if result.stderr and 'UserWarning' not in result.stderr:
            output_text.insert(tk.END, "\n[ERROR] " + result.stderr)
            output_text.see(tk.END)  # Auto-scroll to the bottom

    except Exception as e:
        output_text.insert(tk.END, f"\n[ERROR] {str(e)}")
        output_text.see(tk.END)  # Auto-scroll to the bottom

# Function to run the selected script with the input date and library sheet
def run_script_with_sheet(script_name, input_date_str, library_sheet_filepath):
    try:
        #python_executable = "/Users/aarongarner/Desktop/OTT-SCHEDULE-CREATOR/ott_sched_env/bin/python3"  # For Linux/macOS
        python_executable = sys.executable
        command = [python_executable, os.path.join(os.path.dirname(__file__), script_name), input_date_str, library_sheet_filepath]
        result = subprocess.run(command, capture_output=True, text=True)
        
        output_text.insert(tk.END, result.stdout)  # Insert new output
        output_text.see(tk.END)  # Auto-scroll to the bottom

        if result.stderr and 'UserWarning' not in result.stderr:
            output_text.insert(tk.END, "\n[ERROR] " + result.stderr)
            output_text.see(tk.END)  # Auto-scroll to the bottom

    except Exception as e:
        output_text.insert(tk.END, f"\n[ERROR] {str(e)}")
        output_text.see(tk.END)  # Auto-scroll to the bottom

# Function to get the input date from the calendar and run the active house code grabber script
def get_date_and_run_ahcg(script_name):
    input_date_str = cal.get_date()
    run_script(script_name, input_date_str)

# Function to get the input date from the calendar and run the schedule creator script
def get_date_and_run_schedule_creator(script_name, library_sheet_filepath):
    if not library_sheet_filepath:
        output_text.insert(tk.END, "\nNo OTTera library sheet selected!\n")
        output_text.see(tk.END)  # Auto-scroll to the bottom
        return
    input_date_str = cal.get_date()
    run_script_with_sheet(script_name, input_date_str, library_sheet_filepath)

# Function to handle file upload and store the file path
def upload_file():
    global library_sheet_filepath  # Declare it as global to update the global variable
    filename = filedialog.askopenfilename(filetypes=[("CSV Files", "*.csv")])
    if filename:
        library_sheet_filepath = filename  # Store the filepath in the global variable
        file_label.config(text=filename.split('/')[-1])  # Display only the filename

library_sheet_filepath = ''

# Create the main window
root = tk.Tk()
root.title("OTTera Schedule Creator")

# Make the window resizable
root.grid_rowconfigure(0, weight=1)
root.grid_columnconfigure(0, weight=1)

# Create a label for instructions
label = tk.Label(root, text="Select a date using the calendar,\nthen select the channel to perform the task.")
label.pack(pady=5)

# Frame to hold the calendar and both functional sections
main_frame = tk.Frame(root)
main_frame.pack(pady=10, padx=10)

# Create a calendar widget
cal = Calendar(main_frame, selectmode="day", date_pattern="mm/dd/yyyy")
cal.grid(row=0, column=0, padx=10, pady=10, sticky="n")

# Center the two sections in the single column
main_frame.grid_columnconfigure(0, weight=1)

# Left section (Get Active House Codes)
upper_frame = tk.LabelFrame(main_frame, text="Get Active House Codes", padx=10, pady=10)
upper_frame.grid(row=0, column=1, padx=10, pady=10, sticky="n")

acl_achg_button = tk.Button(upper_frame, text="ACL", command=lambda: get_date_and_run_ahcg('ahcg_acl.py'))
acl_achg_button.pack(pady=5)

bark_achg_button = tk.Button(upper_frame, text="Bark TV", command=lambda: get_date_and_run_ahcg('ahcg_bark.py'))
bark_achg_button.pack(pady=5)

billiard_achg_button = tk.Button(upper_frame, text="Billiard TV", command=lambda: get_date_and_run_ahcg('ahcg_billiard.py'))
billiard_achg_button.pack(pady=5)

boxing_achg_button = tk.Button(upper_frame, text="Boxing TV", command=lambda: get_date_and_run_ahcg('ahcg_boxing.py'))
boxing_achg_button.pack(pady=5)

pll_achg_button = tk.Button(upper_frame, text="PLL", command=lambda: get_date_and_run_ahcg('ahcg_pll.py'))
pll_achg_button.pack(pady=5)

psw_achg_button = tk.Button(upper_frame, text="PowerSports World", command=lambda: get_date_and_run_ahcg('ahcg_psw.py'))
psw_achg_button.pack(pady=5)

# Right section (Get Schedule Sheet)
lower_frame = tk.LabelFrame(main_frame, text="Get Schedule Sheet", padx=10, pady=10)
lower_frame.grid(row=0, column=2, padx=10, pady=10, sticky="n")

# File upload button and label to show filename
file_button = tk.Button(lower_frame, text="Upload Library Sheet", command=upload_file)
file_button.pack(pady=5)

file_label = tk.Label(lower_frame, text="No file selected")
file_label.pack(pady=5)

# Identical buttons for schedule sheet
acl_sched_button = tk.Button(lower_frame, text="ACL", command=lambda: get_date_and_run_schedule_creator('schedule_creator_acl.py', library_sheet_filepath))
acl_sched_button.pack(pady=5)

bark_sched_button = tk.Button(lower_frame, text="Bark TV", command=lambda: get_date_and_run_schedule_creator('schedule_creator_bark.py', library_sheet_filepath))
bark_sched_button.pack(pady=5)

billiard_sched_button = tk.Button(lower_frame, text="Billiard TV", command=lambda: get_date_and_run_schedule_creator('schedule_creator_billiard.py', library_sheet_filepath))
billiard_sched_button.pack(pady=5)

boxing = tk.Button(lower_frame, text="Boxing TV", command=lambda: get_date_and_run_schedule_creator('schedule_creator_boxing.py', library_sheet_filepath))
boxing.pack(pady=5)

pll_dom_sched_button = tk.Button(lower_frame, text="PLL Domestic", command=lambda: get_date_and_run_schedule_creator('schedule_creator_pll_dom.py', library_sheet_filepath))
pll_dom_sched_button.pack(pady=5)

pll_int_sched_button = tk.Button(lower_frame, text="PLL International", command=lambda: get_date_and_run_schedule_creator('schedule_creator_pll_int.py', library_sheet_filepath))
pll_int_sched_button.pack(pady=5)

psw_sched_button = tk.Button(lower_frame, text="PowerSports World", command=lambda: get_date_and_run_schedule_creator('schedule_creator_psw.py', library_sheet_filepath))
psw_sched_button.pack(pady=5)

# Create a frame for the output text and scrollbar
output_frame = tk.Frame(root)
output_frame.pack(pady=10)

# Create a scrollbar for the Text widget
scrollbar = tk.Scrollbar(output_frame)
scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

# Create a Text widget with a larger height
output_text = tk.Text(output_frame, height=25, width=80, yscrollcommand=scrollbar.set)
output_text.pack(side=tk.LEFT)

# Define the font with a larger font size
font = tkFont.Font(family="Helvetica", size=24)  # Change 'size' to your preferred font size

# Configure the scrollbar to work with the Text widget
scrollbar.config(command=output_text.yview)

# Run the GUI event loop
root.mainloop()