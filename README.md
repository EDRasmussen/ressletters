# RessLetters
Small CLI tool to ressurect deadletter messages in an Azure Service Bus queue or topic.

## Usage
```
Usage: ressletters --namespace <hostname> --queue <queue-or-topic> --batchsize <batch-size> [--norand]

Flags:
  -batchsize int
        Number of messages to process at a time (250 seems to be the hard cap) (default 200)
  -namespace string
        Service Bus hostname (e.g., myservicebus.servicebus.windows.net)
  -norand
        Disable random IDs for new messages
  -queue string
        Name of the queue or topic
```

## Download
Check releases
