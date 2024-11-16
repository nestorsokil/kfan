# kfan

For moving shit around Kafka

```
A tool to manage Kafka fan-in and fan-out operations via the command line.

Usage:
  kfan [flags]

Flags:
  -c, --broadcast        Broadcast strategy (default true)
  -b, --brokers string   Kafka brokers if applicable to both 'in' and 'out' (default "localhost:9092")
  -t, --from-tail        Start from the tail of the topic (use 'latest' instead of 'earliest')
  -g, --group string     Consumer group ID (default "kfan-group")
  -h, --help             help for kfan
  -i, --in string        Comma-separated inputs -- (broker?):topic or 'std:in'
  -o, --out string       Comma-separated inputs -- (broker?):topic or 'std:out+headers'
  -r, --round-robin      Round-robin strategy
```
