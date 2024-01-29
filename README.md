# Python-Scripts
A repo of Python scripts to simply issue resolution and simple command generation for confluent Platform

Example of running `python3 parse_topic_describe.py --file <path-to-topic-describe-file>`
```
Count of under replicated partitions: 5
Under replicated partitions:
  Topic: "testing.topic", Partition: "0", Leader Node: "1", Brokers Out of Sync: "Observers: 11"
  Topic: "secondary.topic", Partition: "2", Leader Node: "2", Brokers Out of Sync: "1, Observers: 12"
  Topic: "secondary.topic", Partition: "5", Leader Node: "3", Brokers Out of Sync: "2"
Leadership distribution:
  Broker: 1 is a leader for 10 partition(s)
  Broker: 2 is a leader for 11 partition(s)
  Broker: 3 is a leader for 9 partition(s)
  Broker: 11 is a leader for 12 partition(s)
  Broker: 12 is a leader for 10 partition(s)
  Broker: 13 is a leader for 7 partition(s)
Leadership distribution:
  Broker: 1 is a leader for 311 partition(s)
  Broker: 3 is a leader for 371 partition(s)
  Broker: 13 is a leader for 327 partition(s)
  Broker: 11 is a leader for 339 partition(s)
  Broker: 2 is a leader for 245 partition(s)
  Broker: 12 is a leader for 287 partition(s)
Under replicated leader counts:
  Broker: 1 is a leader for 1 under replicated partition(s)
  Broker: 2 is a leader for 1 under replicated partition(s)
  Broker: 3 is a leader for 1 under replicated partition(s)
Frequency of Non-Preferred leadership:
  Broker: 3 is not the leader while being the preferred leader for 735 partitions(s)
  Broker: 12 is not the leader while being the preferred leader for 238 partitions(s)
  Broker: 11 is not the leader while being the preferred leader for 111 partitions(s)
Frequency of Preferred leadership:
  Broker: 1 is the preferred leader and the actual leader for 1 partitions(s)
  Broker: 2 is the preferred leader and the actual leader for 1 partitions(s)
  Broker: 3 is the preferred leader and the actual leader for 327 partitions(s)
  Broker: 11 is the preferred leader and the actual leader for 47 partitions(s)
  Broker: 12 is the preferred leader and the actual leader for 93 partitions(s)
```
