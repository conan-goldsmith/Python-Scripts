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
    replicas , isr = extract_replicas_and_isr ( line )
    missing_in_isr = replicas - isr
    in_sync = replicas.intersection(isr)
    keyword = "Offline: "
    if keyword in line:
        parts = line.split()
        topic_name, partition, leader_node = parts[1], parts[3], parts[5]
        offline_replicas = line.split(keyword)[-1].split("\n\t")[0].strip()
        if offline_replicas:
            return f"    Topic: \"{topic_name}\", Partition: \"{partition}\", Leader Node: \"{leader_node}\", Brokers in Sync: \"{in_sync}\", Brokers Out of Sync: \"{missing_in_isr}\", Offline Replicasx: \"{offline_replicas}\""
        return None
    return None

def get_under_replicated_partitions ( line , min_isr_value ) :
    """
    Extract offline partition information from a line.

    Args:
        line (str): The line containing offline partition information.

    Returns:
        str or None: Formatted offline partition information, or None if not applicable.
        :param i:
    """
    replicas , isr = extract_replicas_and_isr ( line )
    missing_in_isr = replicas - isr
    in_sync = replicas.intersection(isr)
    if missing_in_isr :
        parts = line.split()
        topic_name, partition, leader_node = parts[1], parts[3], parts[5]
        URP_Output = f"    Topic: \"{topic_name}\", Partition: \"{partition}\", Leader Node: \"{leader_node}\", Brokers in Sync: \"{in_sync}\", Brokers Out of Sync: \"{missing_in_isr}\""
        under_isr_output = None
        if isr and len(isr) < min_isr_value:
           under_isr_output = f"    Under min.insync.replicas:\"{min_isr_value}\", Topic: \"{topic_name}\", Partition: \"{partition}\", Leader Node: \"{leader_node}\", Brokers in Sync: \"{in_sync}\", Brokers Out of Sync: \"{missing_in_isr}\""
        return URP_Output,under_isr_output
    return None, None

def extract_replicas_and_isr(line):
    """
    Extracts the replicas and ISR from a line, excluding any parts of the line after a tab.

    Args:
        line (str): The line from which to extract the information.

    Returns:
        tuple: A set of replicas and a set of ISRs.
    """
    # Extract replicas
    replicas_match = re.search(r"Replicas: ([\d,]+)", line)
    replicas = set(replicas_match.group(1).split(",")) if replicas_match else set()

    # Extract ISR
    isr_match = re.search(r"Isr: ([\d,]+)", line)
    isr = set(isr_match.group(1).split(",")) if isr_match else set()

    return replicas, isr
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
    under_replicated_partitions = []
    under_min_isr = []
    under_replicated_partition_count_by_leader = {}
    leader_counts = {}
    leader_percentage = {}
    non_preferred_leader_counts = {}
    preferred_leader_counts = {}
    min_isr_list = {}
    topic_name =""

    lines = output.split("\n")
    for line in lines:
        isr = None
        if "Topic: " in line :
            topic_name = re.search(r"Topic: (\S+)", line).group(1)
        if "min.insync.replicas" in line :
            min_isr_list[topic_name] = extract_integer_value (line, r"min\.insync\.replicas=" )



        if "Leader: none" in line:
            offline_info = get_offline_partitions(line)
            if offline_info:
                offline_partitions.append(offline_info)


        leader_id = extract_integer_value(line, "Leader: ")
        if leader_id is not None:
            leader_counts[leader_id] = leader_counts.get(leader_id, 0) + 1

            preferred_leader = extract_integer_value(line, "Replicas: ")
            if preferred_leader is not None:
                if leader_id != preferred_leader:
                    non_preferred_leader_counts[leader_id] = non_preferred_leader_counts.get(leader_id, 0) + 1
                else:
                    preferred_leader_counts[leader_id] = preferred_leader_counts.get(leader_id, 0) + 1


            if "Isr: " in line :
                isr_output, min_isr_output = get_under_replicated_partitions(line, min_isr_list[topic_name])
                if isr_output:
                    under_replicated_partitions.append(isr_output)
                    under_replicated_partition_count_by_leader [ leader_id ] = \
                        (under_replicated_partition_count_by_leader.get( leader_id , 0 ) + 1)
                if min_isr_output:
                    under_min_isr.append(min_isr_output)


    total_partitions = sum(leader_counts.values())

    # Count of under replicated partitions
    print(f"There are {total_partitions} partitions, {len(under_replicated_partitions)} Under Replicated Parititons (URP), and {len(offline_partitions)} offline partitions")

    print(f"--------------------------------------------------------------------------------------")
    if  len(under_replicated_partitions) == 0:
        print(f"No Under Replicated Partitions Found")
    else:
        print (
            f"Count of under replicated Partition(s): { len ( under_replicated_partitions )}" )
        print ( "Under replicated partition(s):" )
        for partition_info in under_replicated_partitions:
            print(partition_info)

    if len(offline_partitions) + len(under_min_isr) == 0:
        print(f"No Offline Partitions Found")
    else:
        print (
            f"Count of offline partitions(s): { len ( offline_partitions ) + len(under_min_isr)}" )
        if len(offline_partitions) != 0:
            print (f"Offline partitions(s):" )
            for partition_info in offline_partitions:
                print(partition_info)
        if len(under_min_isr) != 0:
            print (f"Under Min ISR partitions(s):" )
            for partition_info in under_min_isr:
                print(partition_info)

    # Leadership distribution
    print("Leadership distribution:")
    for leader_id, count in leader_counts.items():
        leader_percentage[leader_id] = (count / total_partitions) * 100
        print(f"    Broker: {leader_id} is a leader for {count} partition(s). That is {leader_percentage[leader_id]:.2f} % of all partitions")

    # Under replicated leader counts
    print("Under replicated leader counts:")
    for leader_id, count in under_replicated_partition_count_by_leader.items():
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
