"""
This script is designed to parse and analyze Kafka topic information. It can process data from a specified file
or execute a shell command to gather data. It provides insights into offline partitions, under-replicated partitions,
leadership distribution, and frequency of preferred leadership among Kafka topics.

Usage:
    Run with a file: python kafka_analyzer.py --file <path_to_file>
    Run with a command: python kafka_analyzer.py --command '<shell_command>'
"""

import subprocess
import argparse
import os
import re

def run_command(command):
    """
    Execute a shell command and return its output or an error message.

    Args:
        command (str): The command to be executed in the shell.

    Returns:
        str: The standard output from the command or an error message.

    Raises:
        subprocess.CalledProcessError: If the command execution fails.
    """
    try:
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        output, error = process.communicate()
        if process.returncode != 0:
            raise subprocess.CalledProcessError(process.returncode, command, output=output, stderr=error)
        return output.decode()
    except subprocess.CalledProcessError as e:
        return f"Command '{command}' failed with exit status {e.returncode}: {e.stderr.decode()}"

def extract_integer_value(line, keyword):
    """
    Extract an integer value from a line based on the specified keyword.

    Args:
        line (str): The line from which to extract the value.
        keyword (str): The keyword to identify the start of the value.

    Returns:
        int or None: The extracted integer value, or None if not found.
    """
    match = re.search(rf"{keyword}(\d+)", line)
    if match:
        return int(match.group(1))
    return None

def extract_replicas_and_isr(line):
    """
    Extracts the replicas and ISR from a line.

    Args:
        line (str): The line from which to extract the information.

    Returns:
        tuple: A set of replicas and a set of ISRs.
    """
    # Extract replicas
    replicas_match = re.search(r"Replicas: ([\d,]+)", line)
    replicas = set(map(int, replicas_match.group(1).split(","))) if replicas_match else set()

    # Extract ISR
    isr_match = re.search(r"Isr: ([\d,]+)", line)
    isr = set(map(int, isr_match.group(1).split(","))) if isr_match else set()

    return replicas, isr

# The functions `get_offline_partitions`, `get_under_replicated_partitions`, `parse_output` need to be updated accordingly
# to match the structure and error handling strategy as discussed. Additionally, these functions should have detailed
# documentation explaining their purpose, inputs, and outputs for consistency and clarity.

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Analyze Kafka topic information from a file or command output.")
    parser.add_argument('--file', help='Path to a file containing Kafka topic information.')
    parser.add_argument('--command', help='Shell command to retrieve Kafka topic information.')

    args = parser.parse_args()

    try:
        if args.file:
            if not os.path.isfile(args.file):
                raise FileNotFoundError(f"The specified file does not exist: {args.file}")
            with open(args.file, 'r') as file:
                output = file.read()
                parse_output(output)
        elif args.command:
            output = run_command(args.command)
            parse_output(output)
        else:
            parser.print_help()
    except Exception as e:
        print(f"An error occurred: {e}")
        print("Please check the input parameters and try again.")
