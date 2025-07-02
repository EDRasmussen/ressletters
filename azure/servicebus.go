package azure

import (
	"context"
	"fmt"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
)

type SimpleMessage struct {
	MessageID string
	Body      []byte
}

func GetClient(namespace string) (*azservicebus.Client, error) {
	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return nil, fmt.Errorf("Unable to create credentials: %w", err)
	}

	client, err := azservicebus.NewClient(namespace, cred, nil)
	if err != nil {
		return nil, fmt.Errorf("Unable to create Azure Service Bus client %w", err)
	}

	return client, nil
}

func SendMessageBatch(queueOrTopic string, messages []*SimpleMessage, client *azservicebus.Client) error {
	sender, err := client.NewSender(queueOrTopic, nil)
	if err != nil {
		return fmt.Errorf("Unable to create sender for topic: %w", err)
	}
	defer sender.Close(context.TODO())

	batch, err := sender.NewMessageBatch(context.TODO(), nil)
	if err != nil {
		return fmt.Errorf("Unable to create message batch: %w", err)
	}

	for _, message := range messages {
		if err := batch.AddMessage(&azservicebus.Message{Body: message.Body, MessageID: &message.MessageID}, nil); err != nil {
			return fmt.Errorf("Unable to add message to batch: %w", err)
		}
	}
	if err := sender.SendMessageBatch(context.TODO(), batch, nil); err != nil {
		return fmt.Errorf("Unable to send message batch: %w", err)
	}

	return nil
}

func ReceiveDeadletterMessages(queueOrTopic string, maxCount int, client *azservicebus.Client) ([]*SimpleMessage, error) {
	receiver, err := client.NewReceiverForQueue(
		queueOrTopic,
		&azservicebus.ReceiverOptions{
			SubQueue: azservicebus.SubQueueDeadLetter,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("An error occured when trying to create a receiver: %w", err)
	}
	defer receiver.Close(context.TODO())

	messages, err := receiver.ReceiveMessages(context.TODO(), maxCount, nil)
	if err != nil {
		return nil, fmt.Errorf("An error occured when trying to receive messages: %w", err)
	}

	var simpleMessages []*SimpleMessage
	for _, message := range messages {
		simpleMessages = append(simpleMessages, &SimpleMessage{
			Body:      message.Body,
			MessageID: message.MessageID,
		})
		err := receiver.CompleteMessage(context.TODO(), message, nil)
		if err != nil {
			return nil, fmt.Errorf("An error occured when trying to complete a message: %w", err)
		}
	}

	return simpleMessages, nil
}

func PeekDeadletterMessages(queueOrTopic string, maxCount int, client *azservicebus.Client) ([]*SimpleMessage, error) {
	receiver, err := client.NewReceiverForQueue(
		queueOrTopic,
		&azservicebus.ReceiverOptions{
			SubQueue: azservicebus.SubQueueDeadLetter,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("An error occured when trying to create a receiver: %w", err)
	}
	defer receiver.Close(context.TODO())

	messages, err := receiver.PeekMessages(context.TODO(), maxCount, nil)
	if err != nil {
		return nil, fmt.Errorf("An error occured when trying to receive messages: %w", err)
	}

	var simpleMessages []*SimpleMessage
	for _, message := range messages {
		simpleMessages = append(simpleMessages, &SimpleMessage{
			Body:      message.Body,
			MessageID: message.MessageID,
		})
	}

	return simpleMessages, nil
}
