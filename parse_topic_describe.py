import subprocess
import argparse
import os
import re

def run_command(command):
    """
    Execute a shell command and return its output.

    Args:
        command (str): The command to execute.

    Returns:
        str: The output of the command, or an error message if an error occurs.
    """
    try:
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        output, error = process.communicate()
        if error:
            return f"Error occurred: {error.decode()}"
        return output.decode()
    except Exception as e:
        return f"Error executing command: {str(e)}"

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

def get_offline_partitions(line):
    """
    Extract offline partition information from a line.

    Args:
        line (str): The line containing offline partition information.

    Returns:
        str or None: Formatted offline partition information, or None if not applicable.
    """
    keyword = "Offline: "
    if keyword in line:
        parts = line.split()
        topic_name, partition, leader_node = parts[1], parts[3], parts[5]
        offline_replicas = line.split(keyword)[-1].split("\n\t")[0].strip()
        if offline_replicas:
            return f"\tTopic: \"{topic_name}\", Partition: \"{partition}\", Leader Node: \"{leader_node}\", Brokers Out of Sync: \"{offline_replicas}\""
        return None
    return None

def get_out_of_sync(line):
    """
    Extract out-of-sync replicas from a line.

    Args:
        line (str): The line containing replica information.

    Returns:
        set or None: Set of out-of-sync replicas, or None if not found.
    """
    replicas_match = re.search(r"Replicas:(\S+)", line)
    isr_match = re.search(r"Isr:(\S+)", line)
    if replicas_match and isr_match:
        replicas = set(replicas_match.group(1).split(','))
        isr_list = set(isr_match.group(1).split(','))
        return replicas - isr_list
    return None

def parse_output(output):
    """
    Parse and analyze the output from Kafka's topic describe command.

    Args:
        output (str): The output to be parsed and analyzed.
    """
    offline_partitions = []
    under_replicated_partitions = {}
    leader_counts = {}
    leader_percentage = {}
    non_preferred_leader_counts = {}
    preferred_leader_counts = {}

    lines = output.split("\n")
    for line in lines:
        if "Leader: none" in line or "Offline:" in line:
            offline_info = get_offline_partitions(line)
            if offline_info:
                offline_partitions.append(offline_info)

        leader_id = extract_integer_value(line, "Leader: ")
        if leader_id is not None:
            leader_counts[leader_id] = leader_counts.get(leader_id, 0) + 1

            preferred_leader = extract_integer_value(line, "Replicas:")
            if preferred_leader is not None:
                if leader_id != preferred_leader:
                    non_preferred_leader_counts[leader_id] = non_preferred_leader_counts.get(leader_id, 0) + 1
                else:
                    preferred_leader_counts[leader_id] = preferred_leader_counts.get(leader_id, 0) + 1

            if "Offline:" in line or "Isr:" in line:
                under_replicated_partitions[leader_id] = under_replicated_partitions.get(leader_id, 0) + 1

    total_partitions = sum(leader_counts.values())

    # Count of under replicated partitions
    print(f"Count of under replicated Partition(s): {len(offline_partitions)}")
    print("Under replicated partition(s):")
    for partition_info in offline_partitions:
        print(partition_info)

    # Leadership distribution
    print("Leadership distribution:")
    for leader_id, count in leader_counts.items():
        leader_percentage[leader_id] = (count / total_partitions) * 100
        print(f"    Broker: {leader_id} is a leader for {count} partition(s). That is {leader_percentage[leader_id]:.2f} % of all partitions")

    # Under replicated leader counts
    print("Under replicated leader counts:")
    for leader_id, count in under_replicated_partitions.items():
        print(f"    Broker: {leader_id} is a leader for {count} under replicated partition(s)")

    # Frequency of Preferred leadership
    print("Frequency of Preferred leadership:")
    non_preferred_percentage = sum(non_preferred_leader_counts.values()) / total_partitions * 100
    print(f"    {non_preferred_percentage:.2f} % of all partitions are not led by the preferred replica")
    for leader_id in leader_counts.keys():
        preferred_percent = preferred_leader_counts.get(leader_id, 0) / leader_counts[leader_id] * 100
        non_preferred_percent = non_preferred_leader_counts.get(leader_id, 0) / leader_counts[leader_id] * 100 if leader_counts[leader_id] != 0 else 0
        print(f"    Broker: {leader_id} is leading {preferred_percent:.2f} % of partitions where it is the preferred replica. {non_preferred_percent:.2f} % of its partitions where it is not the preferred replica.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Parse and analyze Kafka topic information.")
    parser.add_argument('--file', help='File containing the output to analyze')
    parser.add_argument('--command', help='Command to execute and analyze its output')
    args = parser.parse_args()

    try:
        if args.file:
            if os.path.isfile(args.file):
                with open(args.file, 'r') as file:
                    output = file.read()
                    parse_output(output)
            else:
                print(f"The file {args.file} does not exist. Please provide a valid file.")
        elif args.command:
            output = run_command(args.command)
            parse_output(output)
        else:
            print("Please provide either a file to read from or a command to run.")
    except Exception as e:
        print(f"Error processing request: {str(e)}")
