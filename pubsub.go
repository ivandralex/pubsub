package pubsub

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/pubsub"

	log "github.com/sirupsen/logrus"
)

//if there are no messages for idleSeconds
const idleSeconds int = 30

func PullMessages(ctx context.Context, projectID, subID string) (chan *pubsub.Message, chan error) {
	// Create a channel to handle messages to as they come in.
	messages := make(chan *pubsub.Message)
	errChan := make(chan error)

	go func() {
		client, err := pubsub.NewClient(ctx, projectID)
		if err != nil {
			close(messages)
			errChan <- fmt.Errorf("pubsub.NewClient: %v", err)
			return
		}
		defer client.Close()

		idleContext, cancel := context.WithCancel(ctx)
		var active bool

		go func() {
			for {
				log.Debugf("Set active to false in %s", subID)
				active = false
				time.Sleep(time.Second * 30)
				if !active {
					log.Debugf("Still not active. Cancel ctx for %s", subID)
					cancel()
					break
				}
				log.Debugf("%s is still active!", subID)
			}
		}()

		sub := client.Subscription(subID)

		// Receive blocks until the context is cancelled or an error occurs.
		err = sub.Receive(idleContext, func(ctx context.Context, msg *pubsub.Message) {
			messages <- msg
			active = true
		})

		if err != nil {
			errChan <- fmt.Errorf("sub receive error: %v", err)
		}

		close(messages)
	}()

	return messages, errChan
}

func PublishMessage(ctx context.Context, projectID, topicID string, message *pubsub.Message) error {
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		return err
	}
	defer client.Close()

	topic := client.Topic(topicID)

	res := topic.Publish(ctx, message)

	_, err = res.Get(ctx)

	return err
}
