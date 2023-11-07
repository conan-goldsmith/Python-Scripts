import concurrent.futures
import os
import pathlib
import re
import shutil
import subprocess
import tarfile
import time
import zipfile
from datetime import datetime
from enum import Enum
import py7zr
import platform


def log_general_message(message, calling_method='Undefined Calling Method', print_to_console=True, string_log_level = 'INFO'):

    log_level = LogLevels[string_log_level.upper()]
    # Check if the log level of the message is allowed with the current configured log level
    if(log_level.value >= global_config.current_log_level.value):
        # Get current date and time to use in log lines
        now = datetime.now()
        current_date_time = now.strftime("[%m-%d-%Y %H:%M:%S]")

        # Join all component of the log message
        full_message = f"{current_date_time} [{log_level.name}]: {message} ({calling_method})"

        if(print_to_console):
            # Print the event to stdout
            print(full_message)

        log_full_message = f"{full_message}\n"
        # Write the message to the output log
        global_config.processing_log_file_handle.write(log_full_message)

def extract_then_move(compresed_object, file_path, file_name, file_type):
    extraction_folder = join_paths_and_convert(global_config.log_parsing_folder_full_path, file_name)
    log_general_message(f"Found {file_type} file {file_name}, starting decompression process into {global_config.log_parsing_folder_full_path}",
                        "Regex Searching: Extracting Files")
    os.makedirs(extraction_folder, exist_ok=True)
    compresed_object.extractall(extraction_folder)
    log_general_message(f"Decompressed {file_type} file {file_name} to {extraction_folder}",
                        "Regex Searching: Extracting Files")
    shutil.move(file_path, global_config.compressed_files_folder_full_path)

def extract_file(file_path):
    file_name = pathlib.Path(file_path).stem

    if zipfile.is_zipfile(file_path):
        file_type = 'Zip'
        with zipfile.ZipFile(file_path, 'r') as zip_ref:
            extract_then_move(zip_ref, file_path, file_name, file_type)

    elif tarfile.is_tarfile(file_path):
        file_type = 'Tar'
        with tarfile.open(file_path, 'r:gz') as tar_ref:
            extract_then_move(tar_ref, file_path, file_name, file_type)

    elif file_path.endswith('.7z'):
        file_type = '7Zip'
        with py7zr.SevenZipFile(file_path, mode='r') as seven_zip:
            extract_then_move(seven_zip, file_path, file_name, file_type)

    else:
        log_general_message(f"No decompression performed on file {file_name} as it is not a zip, tar.gz, or 7zip file", "Regex Searching: Extracting Files")


def run_command(command):
    """
    Runs a command using subprocess, and runs it in WSL if the OS is Windows.
    """
    if global_config.host_os_platform == OS.WINDOWS:
        command = "wsl " + command
    return subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)

def grep_pattern(pattern, file_path):
    log_general_message(f"Processing '{pattern}' on the files '{file_path}'", "Regex Searching: Grep Commands")
    # Convert bytes to string for grep command
    pattern_str = pattern.decode('utf-8')
    # Use grep to search for the pattern
    grep_command = f"grep -P '{pattern_str}' {file_path}"

    # Execute the command
    process = run_command(grep_command)
    # Get the output
    output, error = process.communicate()
    output_str = output.decode('utf-8')
    # Count the number of occurrences of identical matches
    lines = output_str.split('\n')

    results = "\n".join(f"{line}" for line in lines)

    # If no results send back None
    if len(results) == 0:
        return None
    else:
        return f"Results for pattern '{pattern_str}' in file '{file_path}: \n{results}\n"

def sed_patterns( file_path, dict = [],):
    """
    Performs a global search and replace using regular expressions on a file
    using subprocess.Popen.
    """
    # log_general_message(f"Processing '{pattern}' on the files '{file_path}'", "Sed Summary: Sed Commands")
    log_general_message(f"Processing file '{file_path}'", "Sed Summary: Sed Commands")
    # Convert bytes to string for grep command

    patterns = f"grep -v \"Results for pattern\" {file_path} | sed -E '"
    # replace with dict being passed in instead of hard coded
    for pattern, replace in global_config.sed_replacements_for_summary:
        # pattern_str = pattern.decode('utf-8')
        # replace_str = replace.decode('utf-8')
        patterns += f" s/{pattern}/{replace}/g ; "
    # Use sed to search for the pattern, all commands are performed on global replacements
    sed_command = f"{patterns}'  | sort |  uniq -c | sort -r"

    print(f"Full Pattern:  {sed_command} \n")

    # Execute the command
    process = run_command(sed_command)
    # Get the output
    output, error = process.communicate()
    output_str = output.decode('utf-8')
    # Count the number of occurrences of identical matches
    lines = output_str.split('\n')

    results = "\n".join(f"{line}" for line in lines)

    # If no results send back None
    if len(results) == 0:
        return None
    else:
        return (f"Frequency of events of pattern '{pattern}' in file '{file_path}\n"
                f"Count  |  Pattern Searched '{pattern}': \n{results}\n")

def extract_all_directory():
    for root, dirs, files in os.walk(global_config.input_folder_full_path):

        root = convert_wsl_paths(root)

        log_general_message(f"Extracting files in directory {root}",
                            "Extracting Folders: Extract All Dirs")
        if root in global_config.excluded_folders_full_path:  # Skip if the root is in the excluded folders
            log_general_message(f"Skiping {root} due to being in the excluded folder paths",
                                "Extracting Folders: Extract All Dirs")
            continue

        for file in files:
            file_path = join_paths_and_convert(root, file)

            log_general_message(f"Processing {file_path}",
                                "Extracting Folders: Extract All Dirs")
            if any(excluded_folder in file_path for excluded_folder in global_config.excluded_folders_full_path):  # Skip if the file is in the excluded folders

                log_general_message(f"Skipping {file_path} due to being in the excluded folder paths",
                                    "Extracting Folders: Extract All Dirs")
                continue
            extract_file(file_path)

def grep_all_in_directory(pattern):
    futures = []
    for root, dirs, files in os.walk(global_config.log_parsing_folder_full_path):
        for file in files:
            file_path = join_paths_and_convert(root, file)
            future = process_file(pattern, file_path)
            futures.append(future)
    return futures

def process_file( pattern, file_path):
    future = grep_pattern(pattern, file_path)
    return future

def move_files_except():
    for file_name in os.listdir(global_config.input_folder_full_path):
        if file_name not in global_config.excluded_folders:
            source_folder = join_paths_and_convert(global_config.input_folder_full_path, file_name)
            destination_folder = join_paths_and_convert(global_config.log_parsing_folder_full_path, file_name)
            if os.path.isdir(source_folder):
                shutil.move(source_folder, destination_folder)
            else:
                shutil.move(source_folder, destination_folder)
def recursive_file_dir_traversal(path):
    for root, dirs, files in os.walk(path):
        for file in files:
            print(join_paths_and_convert(root, file))


def create_and_move_folders():
    # Extract any compressed directories
    log_general_message(f"Extracting any compressed files in the provided directory {global_config.input_folder_full_path}",
                        "Extracting Folders: Main Method")
    extract_all_directory()

    log_general_message(f"Moving files into {global_config.log_parsing_folder_full_path} for processing",
                        "Moving Files: Main Method")
    move_files_except()

def find_all_grep_results():
    log_general_message(f"Starting event extraction based on provided REGEX", "Regex Searching: Moving Files")
    # Use a ThreadPoolExecutor to execute the grep commands in parallel
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = {executor.submit(grep_all_in_directory, pattern) for pattern in
                   global_config.regex_patterns}

        # Open the log file in the directory that the grep command is searching
        with open(global_config.full_search_output_log_full_path, 'w') as full_search_output:
            for future in concurrent.futures.as_completed(futures):
                result = future.result()

                # If None then there were no results, so skip the output
                if result is None:
                    continue
                # If the result is full of ONLY None
                if result.count(None) == len(result):
                    continue

                # Write the result to the log file
                for message in result:
                    # If the message is None skip to find any values that are not None
                    if message is None:
                        continue
                    full_search_output.write(message)
                    # Print the result
                    log_general_message(message, "Regex Searching: Grep Commands", False)
                # Print an extra tw blank lines in the log after printing the results
                spacer_lines = f'--------------------\n\n'
                full_search_output.write(spacer_lines)
                log_general_message(spacer_lines, "Regex Searching: Spacer Lines", False)

def sed_output_to_summary():
    log_general_message(f"Building  event extraction based on provided REGEX", "Regex Searching: Moving Files")
    # Use a ThreadPoolExecutor to execute the sed commands in order, as they build on each other
    # Open the log file in the directory that the sed command is reading from and writing to
    with open(global_config.result_summary_log_full_path, 'w') as summary_output:
        # for pattern, replace in global_config.sed_replacements_for_summary:
        result = sed_patterns(global_config.full_search_output_log_full_path, [])

        # # If None then there were no results, so skip the output
        # if result is None:
        #     continue
        # # If the result is full of ONLY None
        # if result.count(None) == len(result):
        #     continue

        if result is None:
            log_general_message("No Results found in Sed Operations", "Sed Summary: Sed Commands", False)
        # Write the result to the log file
        for message in result:
            # If the message is None skip to find any values that are not None
            if message is None:
                continue
            summary_output.write(message)
            # Print the result
            # log_general_message(message, "Sed Summary: Sed Commands", False)
        # Print an extra tw blank lines in the log after printing the results
        spacer_lines = f'--------------------\n\n'
        summary_output.write(spacer_lines)
        # log_general_message(spacer_lines, "Sed Summary: Sed Commands", False)

def windows_to_wsl_path(path):
    """
    Converts a Windows path to a WSL path.
    """
    if path[1] == ":" and path[2] == "\\":
        wsl_path = "/mnt/" + path[0].lower() + path[2:].replace("\\", "/")
        return wsl_path
    else:
        return path

def join_paths_and_convert(path_root, path_end):
    """
    Joins two paths then converts any Windows pathing
    """
    path = os.path.join(path_root, path_end)
    if global_config:
        HOST_OS = global_config.host_os_platform
    else:
        HOST_OS = detect_os()

    if HOST_OS == OS.WINDOWS:
        path = convert_wsl_paths(path)
        return path
    else:
        return path

def convert_wsl_paths(path):
    path = os.path.abspath(path).replace("\\", "/")
    return path

class OS(Enum):
    WINDOWS = "Windows"
    LINUX = "Linux"
    WSL = "WSL"

def detect_os():
    """
    Detects the operating system and returns an enum value.
    """
    if platform.system() == "Windows":
        return OS.WINDOWS
    elif "Microsoft" in platform.release():
        return OS.WSL
    else:
        return OS.LINUX

class LogLevels(Enum):
    NONE = 00
    ALL = 10
    TRACE = 20
    DEBUG = 30
    INFO = 40
    WARN = 50
    ERROR = 60
    FATAL = 60


def replace_backslash(arr):
    """
    Replaces all instances of '\' followed by a number with '\\' in a list of strings or tuples.
    """
    new_arr = []
    for item in arr:
        # if isinstance(item, str):
        #     new_arr.append(item.replace('\\', '\\\\'))
        # elif isinstance(item, tuple):
        pattern, replace = item
        new_arr.append((re.sub(r"\\", r"\\\\", pattern), replace))
    return new_arr



class GlobalConfig():
    def __init__(self):
        # Define logging levels for later use
        self.current_log_level = LogLevels['info'.upper()]
        # TODO check that current_log_level is valid within the log_levels when read from configs

        # Record the start time to report total processing time
        self.processing_start_time = time.time()

        # Detect the platform
        self.host_os_platform = detect_os()

        # Define the path to the default parent path
        self.default_parent_foler_path = '/mnt/c/Users/ConanGoldsmith/Downloads/'
        self.default_parent_foler_path = windows_to_wsl_path(self.default_parent_foler_path)

        # Define name of the input folder
        self.input_foler_name = '183978'

        # Define the path to the input folder
        self.input_folder_full_path = join_paths_and_convert(self.default_parent_foler_path, self.input_foler_name)

        # Create a timestamp for output logs to indicate when the results are from
        now = datetime.now()  # current date and time
        self.date_time = now.strftime("%m_%d_%Y-%H_%M_%S")

        # Define the name and full path to directory used to store all extracted logs
        self.log_parsing_folder_name = 'Logs'
        self.log_parsing_folder_full_path = join_paths_and_convert(self.input_folder_full_path, self.log_parsing_folder_name)

        # Folder to move all compress folders after they have been decompressed
        self.compressed_files_folder_name = 'Archive_of_Compressed_Files'
        self.compressed_files_folder_full_path = join_paths_and_convert(self.input_folder_full_path, self.compressed_files_folder_name)

        # Define the name and full path to directory used to store all results
        self.results_folder_name = 'Processing_Results'
        self.results_folder_full_path = join_paths_and_convert(self.input_folder_full_path, self.results_folder_name)

        # Define the name and full path to logs from running process
        self.processing_log_name = f'Python_Regex_Searching_Log_File_{self.date_time}.log'
        self.processing_log_full_path = join_paths_and_convert(self.results_folder_full_path, self.processing_log_name)


        # Define the name and full path to directory used to store all extracted logs
        self.results_summary_log_name = f'Patterns-in-Results_{self.date_time}.log'
        self.result_summary_log_full_path = join_paths_and_convert(self.results_folder_full_path, self.results_summary_log_name)

        # Define the name and full path to directory used to store all extracted logs
        self.full_search_output_log = f'Parsed-Results_{self.date_time}.log'
        self.full_search_output_log_full_path = join_paths_and_convert(self.results_folder_full_path, self.full_search_output_log)

    def finalize_configs(self):
        # Folders and files to exclude from any moving operations
        self.excluded_folders = [self.log_parsing_folder_name, self.compressed_files_folder_name, self.results_folder_name]


        # Generate the full path for the excluded folders
        self.excluded_folders_full_path = []
        for folder in self.excluded_folders:
            full_path_to_excluded_folder = join_paths_and_convert(self.input_folder_full_path, folder)
            self.excluded_folders_full_path.append(full_path_to_excluded_folder)
            folder_exists = os.path.exists(full_path_to_excluded_folder)
            # log_general_message(f"Processing file_path {self.input_folder_full_path} that contains {folder_contents}",
            #                          "Required Directory Creation: Main Method")
            if not folder_exists:
                os.makedirs(full_path_to_excluded_folder)
                # log_general_message(f"Created required folder for processing {full_path_to_excluded_folder}",
                #                      "Required Directory Creation: Main Method")


        # Open the processing log to remove overhead of freqently reopening the file for each event
        self.processing_log_file_handle = open(self.processing_log_full_path, 'w+')

        # Patterns for Regex searches to perform
        self.regex_patterns = [
            b'.*\d{4}-\d{2}-\d{1,2} \d{1,2}:\d{2}:\d{2},\d{3}.( ERROR | FATAL ).*|(org.apache.kafka.common.errors.*)',
            b'.*(WARN.*)',
            b'(.*client.id.=.(.*))',
            b'^.*(Config values:.).*\(.*',
            b'(.*application.id.=.(.*))',
            b'(ReplicaFetcher.*Error sending fetch request.*)',
            b'.*(brokerId=\d+.*Connection with.*disconnected.*)',
            b'.*java.io.IOException.*(Connection to (.*) failed.*)'
        ]

        # Patterns for sed searches to perform
        # self.regex_patterns = [
        #     b'/Config values:/,/^[^[:space:]]/ {/^[^[:space:]]/d; p} '
        # ]

        # List of pairs (pattern, replacement), these should be performed one at a time, in sequence with pipes between
        self.sed_replacements_for_summary = [
            (r'\[[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2},[0-9]{3}\]', ''),# Needs to be a global replace /g
            (r'((blocked for| Negative message latency|at offset|correlation id|timed out at|deadlineMs|nextAllowedTryMs|partition|offset|changelog-|generationId=)(|=| |=-))([0-9]{1,13})/\1#########/g ;s/(task(s?))( | ID .| .?|Id=)[0-9]{1,5}_[0-9]{1,5}.?', r'\1 ID #########'), # Needs to be a global replace /g
            (r'[0-9]{1,13} attempts left', r'########## attempts left'),
            (r'(partition )(.*)(-[0-9]{1,4})', r'\1  TOPIC_NAME'),

            (r'(_C.AS_\S*)_[0-9]{1,3}', r'\1_UUID'),
            (r'(vert.x-eventloop-thread)(.*)(,main)', r'\1-###-\3'),
            (r'.{8}-.{4}-.{4}-.{4}-.{12}(-StreamThread)?(-.{1,2})?', r'THREAD_UUID\1')
        ]
        # self.sed_replacements_for_summary = replace_backslash(self.sed_replacements_for_summary)

        #  grep -v "were supplied but are not used yet." |
        #  sed -E
        #  's/\[[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2},[0-9]{3}\]// ;s/((blocked for| Negative message latency|at offset|correlation id|timed out at|deadlineMs|nextAllowedTryMs|partition|offset|changelog-|generationId=)(|=| |=-))([0-9]{1,13})/\1#########/g ;s/(task(s?))( | ID .| .?|Id=)[0-9]{1,5}_[0-9]{1,5}.?/\1 ID ##### /g
        #  ; s/(_C.AS_\S*)_[0-9]{1,3}/\1_UUID/g ;s/[0-9]{1,13} attempts left/########## attempts left/ ;s/(partition )(.*)(-[0-9]{1,4})/\1  TOPIC_NAME/ ; s/.{8}-.{4}-.{4}-.{4}-.{12}(-StreamThread)?(-.{1,2})?/THREAD_UUID\1/g; s/(vert.x-eventloop-thread)(.*)(,main)/\1-###-\3/' | sort |  uniq -c | sort -r)" > Processing_Results/Summarized.log


        # sed -E ' s/\[[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2},[0-9]{3}\]//g ;  s/((blocked for| Negative message latency|at offset|correlation id|timed out at|deadlineMs|nextAllowedTryMs|partition|offset|changelog-|generationId=)(|=| |=-))([0-9]{1,13})/\1#########/g ;s/(task(s?))( | ID .| .?|Id=)[0-9]{1,5}_[0-9]{1,5}.?/\1 ID #########/g ;  s/[0-9]{1,13} attempts left/########## attempts left/g ;  s/(partition )(.*)(-[0-9]{1,4})/\1  TOPIC_NAME/g ;  s/(_C.AS_\S*)_[0-9]{1,3}/\1_UUID/g ;  s/(vert.x-eventloop-thread)(.*)(,main)/\1-###-\3/g ;  s/.{8}-.{4}-.{4}-.{4}-.{12}(-StreamThread)?(-.{1,2})?/THREAD_UUID\1/g ; '  | sort |  uniq -c | sort -r

        # List of pairs (pattern, replacement), these should be performed one at a time, in sequence with pipes between
        # self.sed_kstream_replacement_for_summary = [
        #     ('(task(s?))( | ID .| .?|Id=)[0-9]{1,5}_[0-9]{1,5}.?', '\1 ID ##### '),
        #     ('(C.AS_\S*)_[0-9]{1,3}', '\1_UUID'),
        #     ('(vert.x-eventloop-thread)(.*)(,main)', '\1-###-\3'),
        #     ('.{8}-.{4}-.{4}-.{4}-.{12}(-StreamThread)?(-.{1,2})?', 'THREAD_UUID\1')
        # ]


        log_general_message(f'Finished initializing required configurations at process init', 'Main Method: Init Process')

def init_global_configs():
    global global_config
    global_config = GlobalConfig()
    global_config.finalize_configs()



def close_application():
    log_general_message("Process took --- %s seconds ---" % (time.time() - global_config.processing_start_time), 'Main Method: Closing application')
    global_config.processing_log_file_handle.close()


global_config = None
def main():
    """
    The main function.
    """
    init_global_configs()
    # Create a global instance of the configuration object
    # Create any required folder, extract compressed data, and clean the top level of the input folder
    create_and_move_folders()
    # Perform the grep across all the logs based on the defined regex patterns
    find_all_grep_results()

    sed_output_to_summary()
    # Cleanup and close the application
    close_application()

    return


# Using the special variable
# __name__
if __name__ == "__main__":
    main()
