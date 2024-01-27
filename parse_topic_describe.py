import subprocess
import argparse
import os

def run_command(command):
    process = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True)
    output, error = process.communicate()
    if error:
        print(f"Error occurred: {error}")
    return output.decode()

def get_broker_id_from_line(line):
    keyword = "Broker id: "
    start = line.find(keyword)
    if start != -1:
        start += len(keyword)
        end = line.find(" ", start)  # assuming the broker id ends with a space
        if end == -1:  # if there's no space, take until the end of the line
            end = len(line)
        broker_id = line[start:end]
        return int(broker_id)  # convert the broker id to an integer
    else:
        return None  # return None if the keyword isn't found in the line

def get_leader_id_from_line(line):
    keyword = "Leader: "
    start = line.find(keyword)
    if start != -1:
        start += len(keyword)
        end = start
        while end < len(line) and not line[end].isspace():  # find the next whitespace character
            end += 1
        leader_id = line[start:end].strip()  # strip any trailing spaces

        return leader_id  # convert the leader id to an integer
    else:
        return None  # return None if the keyword isn't found in the line

def get_preferred_leader(line):
    keyword = "Isr: "
    start = line.find(keyword)
    if start != -1:
        start += len(keyword)
        end = line.find(",", start)
        if end == -1:  # if there's no comma, take until the end of the line
            end = len(line)
        preferred_replica = line[start:end].strip()  # strip any trailing spaces

        return preferred_replica  # convert the leader id to an integer
    else:
        return None  # return None if the keyword isn't found in the line

def get_preferred_replica_id_from_line(line):
    keyword = "Isr: "
    start = line.find(keyword)
    if start != -1:
        start += len(keyword)
        end = line.find(",", start)  # assuming the preferred replica id ends with a comma
        if end == -1:  # if there's no comma, take until the end of the line
            end = len(line)
        preferred_replica_id = line[start:end]
        return int(preferred_replica_id)  # convert the preferred replica id to an integer
    else:
        return None  # return None if the keyword isn't found in the line

def get_offline_partitions(line):
    keyword = "Offline: "
    start = line.find(keyword)
    result = None
    offline_replicas = None
    if start != -1:
        start += len(keyword)
        end = line.find(",", start)  # assuming the offline replica id ends with a comma
        if end == -1:  # if there's no comma, take until the end of theline
            end = len(line)
        offline_replicas = line[start:end].strip()  # strip to remove any leading/trailing whitespace

        # Extract TopicName, Partition, Leader Node, and Brokers Out of Sync from the line
        topic_name = line.split()[1]
        partition = line.split()[3]
        leader_node = line.split()[5]

        # Format the results
        result = f"    TopicName: \"{topic_name}\", Partition: \"{partition}\", Leader Node: \"{leader_node}\", Brokers Out of Sync: \"{offline_replicas}\""

    return result if offline_replicas else None

def parse_output(output):
    offline_partitions = []
    under_replicated_partitions = []
    leader_counts = {}
    under_replicated_leader_counts = {}
    non_preferred_leader_counts = {}
    preferred_leader_counts = {}


    lines = output.split("\n")
    for line in lines:
        # Parse the line for the information you need
        # This will depend on the exact format of the kafka-topics command output
        # You might need to use regular expressions or other string parsing techniques

        # If the line indicates an offline partition, add it to the list
        if "Leader: none" in line:
            offline_partitions.append(line)

        if "Offline:" in line:
            offline_nodes = get_offline_partitions ( line )
            if offline_nodes is not None:
                leader_id = get_leader_id_from_line(line)
                under_replicated_leader_counts [ leader_id ] = under_replicated_leader_counts.get(leader_id, 0) + 1
                # print(offline_nodes)
                under_replicated_partitions.append (offline_nodes)

        # If the line indicates a broker is a leader for a partition, increment its count
        if "Leader:" in line:
            leader_id = get_leader_id_from_line(line)  # 
            leader_counts[leader_id] = leader_counts.get(leader_id, 0) + 1

        # If the line indicates a partition does not have the preferred replica as the leader, increment the count for the broker
        if "Isr:" in line and "Leader:" in line:
            leader_id = get_leader_id_from_line(line)
            preferred_leader = get_preferred_replica_id_from_line(line)  # You'll need to implement these functions
            if int(leader_id) != int(preferred_leader):
                non_preferred_leader_counts[preferred_leader] = non_preferred_leader_counts.get(preferred_leader, 0) + 1
            else:
                preferred_leader_counts[preferred_leader] = preferred_leader_counts.get(preferred_leader, 0) + 1


    # Count of under replicated partitions
    count = len ( under_replicated_partitions )
    if count != 0:
        print(f"Count of under replicated Partition(s): {count}")
        # Print each under replicated partition
        print ( f"Under replicated partition(s):" )
        for URP in under_replicated_partitions :
            print ( URP )
    else:
        print(f"No Under Replicated Partition(s) Found")

    # Count of under replicated partitions
    if leader_counts is not None:
        print ( f"Leadership distribution:" )
        for leader in leader_counts :
            print ( "    Broker:", leader, "is a leader for", leader_counts.get(leader,0), "partition(s)" )
    else:
        print(f"Could not calculate the count of partition leadership per node")


    # Count of under replicated partitions
    count = len ( under_replicated_leader_counts )
    if count != 0:
        print ( f"Under replicated leader counts:" )
        for leader_count in under_replicated_leader_counts :
            print ( "    Broker:", leader_count, "is a leader for", under_replicated_leader_counts.get(leader_count,0), "under replicated partition(s)" )
    else:
        print(f"No under replicated partition(s) found owned by any specific broker")



    count = len ( non_preferred_leader_counts )
    if count != 0:
        print ( f"Frequency of Non-Preferred leadership:" )
        for leader_id in non_preferred_leader_counts :
            print ( "    Broker:", leader_id, "is not the leader while being the preferred leader for", non_preferred_leader_counts.get(leader_id,0), "partition(s)" )

        print ( f"Frequency of Preferred leadership:" )
        for leader_id in preferred_leader_counts :
            print ( "    Broker:", leader_id, "is the preferred leader and the actual leader for", preferred_leader_counts.get(leader_id,0), "partition(s)" )

    else:
        print(f"All partition(s) were found to be led to by the preferred leader")


parser = argparse.ArgumentParser()
parser.add_argument('--file', help='File to read the output from')
parser.add_argument('--command', help='Command to run if no file is provided')
args = parser.parse_args()

if args.file and os.path.isfile(args.file):
    with open(args.file, 'r') as file:
        output = file.read()
        parse_output(output)
elif args.command:
    output = run_command(args.command)
    parse_output(output)
else:
    print("Please provide either a file to read from or a command to run.")
