package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/pkg/errors"
)

func NewProducer(name string, config kafka.ConfigMap) (TopicProducer, error) {
	slog.Debug(fmt.Sprint("Producing to topic ", name, " with producer config ", config))
	p, err := kafka.NewProducer(&config)
	if err != nil {
		return TopicProducer{}, errors.Wrap(err, "failed to create producer")
	}
	return TopicProducer{
		Name:     name,
		producer: p,
	}, nil
}

func (t TopicProducer) Push(ctx context.Context, msg Message) error {
	deliveryChan := make(chan kafka.Event)
	defer close(deliveryChan)
	if err := t.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &t.Name, Partition: kafka.PartitionAny},
		Value:          msg.Value,
		Key:            msg.Key,
		Headers:        headers(msg.Headers),
	}, deliveryChan); err != nil {
		return errors.Wrap(err, "failed to produce message")
	}
	slog.Debug(fmt.Sprintf("Pushed message to %s", t.Name))
	event := <-deliveryChan
	slog.Debug(fmt.Sprintf("Delivered message to %s", t.Name))
	return errors.Wrap(event.(*kafka.Message).TopicPartition.Error, "failed to deliver message")
}

type TopicConsumer struct {
	Name     string
	consumer *kafka.Consumer
}

func NewConsumer(name string, config kafka.ConfigMap) (TopicConsumer, error) {
	consumer, err := kafka.NewConsumer(&config)
	if err != nil {
		return TopicConsumer{}, errors.Wrap(err, "failed to create consumer")
	}
	slog.Debug(fmt.Sprint("Subscribing to topic ", name, " with consumer config ", config))
	if err := consumer.SubscribeTopics([]string{name}, nil); err != nil {
		return TopicConsumer{}, errors.Wrap(err, "failed to subscribe to topic")
	}
	return TopicConsumer{name, consumer}, nil
}

func (t TopicConsumer) PullChannel(ctx context.Context) <-chan Message {
	pipe := make(chan Message)
	go func() {
		defer close(pipe)
		for {
			msg, err := t.consumer.ReadMessage(10 * time.Second) // todo configurable
			if err == nil {
				slog.Debug(fmt.Sprintf("Read message on %s[%d]", t.Name, msg.TopicPartition.Partition))
				pipe <- Message{
					Key:     msg.Key,
					Value:   msg.Value,
					Headers: headersAsMap(msg.Headers),
					Processed: func() {
						slog.Debug(fmt.Sprintf("Committing message on %s[%d]",
							t.Name, msg.TopicPartition.Partition))
						t.consumer.CommitMessage(msg)
					},
				}
			} else if !err.(kafka.Error).IsTimeout() {
				slog.Error(fmt.Sprintf("Consumer error: %v (%v)\n", err, msg))
				break
			} else if err.(kafka.Error).IsTimeout() {
				slog.Debug(fmt.Sprintf("Consumer timeout: %v\n", err))
			}
		}
	}()
	return pipe
}

func headers(values map[string][]byte) []kafka.Header {
	var kafkaHeaders []kafka.Header
	for key, value := range values {
		kafkaHeaders = append(kafkaHeaders, kafka.Header{Key: key, Value: value})
	}
	return kafkaHeaders
}

func headersAsMap(headers []kafka.Header) map[string][]byte {
	result := make(map[string][]byte, len(headers))
	for _, header := range headers {
		result[header.Key] = header.Value
	}
	return result
}
