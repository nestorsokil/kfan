package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/pkg/errors"
)

func NewProducer(name string, config kafka.ConfigMap) (*TopicProducer, error) {
	slog.Debug(fmt.Sprint("Producing to topic ", name, " with producer config ", config))
	p, err := kafka.NewProducer(&config)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create producer")
	}
	return &TopicProducer{
		Name:     name,
		producer: p,
	}, nil
}

func (t *TopicProducer) Push(ctx context.Context, msg Message) error {
	done := make(chan struct{})
	deliveryChan := make(chan kafka.Event)
	go func() {
		defer close(deliveryChan)
		for e := range deliveryChan {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error == nil {
					slog.Debug(fmt.Sprintf("Delivered message to %s", t.Name))
					done <- struct{}{}
					return
				}
			}
		}
	}()
	var kafkaHeaders []kafka.Header
	for key, value := range msg.Headers {
		kafkaHeaders = append(kafkaHeaders, kafka.Header{
			Key:   key,
			Value: value,
		})
	}
	if err := t.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &t.Name, Partition: kafka.PartitionAny},
		Value:          msg.Value,
		Key:            msg.Key,
		Headers:        kafkaHeaders,
	}, deliveryChan); err != nil {
		return errors.Wrap(err, "failed to produce message")
	}

	t.producer.Flush(1000)

	slog.Debug(fmt.Sprintf("Pushed message to %s", t.Name))
	<-done
	return nil
}

type TopicConsumer struct {
	Name     string
	consumer *kafka.Consumer
}

func NewConsumer(name string, config kafka.ConfigMap) (*TopicConsumer, error) {
	consumer, err := kafka.NewConsumer(&config)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create consumer")
	}
	slog.Debug(fmt.Sprint("Subscribing to topic ", name, " with consumer config ", config))
	if err := consumer.SubscribeTopics([]string{name}, nil); err != nil {
		return nil, errors.Wrap(err, "failed to subscribe to topic")
	}
	return &TopicConsumer{name, consumer}, nil
}

func (t *TopicConsumer) PullChannel(ctx context.Context) <-chan Message {
	pipe := make(chan Message, 100)
	go func() {
		defer close(pipe)
		for {
			msg, err := t.consumer.ReadMessage(time.Second) // todo configurable
			if err == nil {
				slog.Debug(fmt.Sprintf("Read message on %s[%d]", t.Name, msg.TopicPartition.Partition))
				genericHeaders := make(map[string][]byte, len(msg.Headers))
				for _, header := range msg.Headers {
					genericHeaders[header.Key] = header.Value
				}
				done := make(chan struct{})
				pipe <- Message{
					Key:       msg.Key,
					Value:     msg.Value,
					Headers:   genericHeaders,
					processed: done,
				}
				go func() {
					<-done
					slog.Debug(fmt.Sprintf("Committing message on %s[%d]", t.Name, msg.TopicPartition.Partition))
					t.consumer.CommitMessage(msg)
				}()
			} else if !err.(kafka.Error).IsTimeout() {
				slog.Error(fmt.Sprintf("Consumer error: %v (%v)\n", err, msg))
				break
			}
		}
	}()
	return pipe
}
