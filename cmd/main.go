package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"ressletters/azure"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
)

const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func main() {
	namespace := flag.String("namespace", "", "Service Bus hostname (e.g., myservicebus.servicebus.windows.net)")
	queueOrTopic := flag.String("queue", "", "Name of the queue or topic")
	batchSize := flag.Int("batchsize", 200, "Number of messages to process at a time (250 seems to be the hard cap)")
	disableRandomId := flag.Bool("norand", false, "Disable random IDs for new messages")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s --namespace <hostname> --queue <queue-or-topic> --batchsize <batch-size> [--norand]\n", os.Args[0])
		fmt.Fprintln(os.Stderr, "\nFlags:")
		flag.PrintDefaults()
	}

	flag.Parse()

	if *namespace == "" || *queueOrTopic == "" {
		fmt.Fprintln(os.Stderr, "Error: --namespace and --queue are required.")
		flag.Usage()
		os.Exit(1)
	}

	client, err := azure.GetClient(*namespace)
	if err != nil {
		fmt.Printf("An error occured while trying to get Azure Service Bus client: %v\n", err)
		os.Exit(1)
	}

	// Cap is about 250 messages per peek/receive
	oldCount := 999
	for oldCount > 0 {
		oldCount, err = resendBatch(queueOrTopic, batchSize, client, disableRandomId)
		if err != nil {
			fmt.Printf("An error occured while trying to get dead lettered messages: %v\n", err)
			os.Exit(1)
		}
	}

	fmt.Println("Queue stopped responding with new messages. Exiting.")
	os.Exit(0)
}

func resendBatch(queueOrTopic *string, batchSize *int, client *azservicebus.Client, disableRandomId *bool) (int, error) {
	fmt.Printf("Requesting %d messages\n", *batchSize)
	messages, err := azure.ReceiveDeadletterMessages(*queueOrTopic, *batchSize, client)
	if err != nil {
		return 0, err
	}

	fmt.Printf("Received %d/%d messages\n", len(messages), *batchSize)
	if !*disableRandomId {
		messages = generateRandomIds(messages)
	}

	err = azure.SendMessageBatch(*queueOrTopic, messages, client)
	if err != nil {
		return 0, err
	}

	fmt.Printf("Sent %d messages\n", len(messages))
	return len(messages), nil
}

func generateRandomIds(messages []*azure.SimpleMessage) []*azure.SimpleMessage {
	for _, message := range messages {
		oldId := message.MessageID
		for oldId == message.MessageID {
			message.MessageID = generateRandomString(8)
		}
	}

	return messages
}

func generateRandomString(length int) string {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	var sb strings.Builder
	for i := 0; i < length; i++ {
		sb.WriteByte(charset[r.Intn(len(charset))])
	}
	return sb.String()
}
