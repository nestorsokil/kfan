package main

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"strings"
	"sync"

	"log/slog"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

type Strategy int

const (
	Broadcast Strategy = iota
	RoundRobin
)

type Message struct {
	Key     []byte
	Value   []byte
	Headers map[string][]byte

	processed chan<- struct{}
}

type Input interface {
	PullChannel(context.Context) <-chan Message
}

type Output interface {
	Push(context.Context, Message) error
}

type TopicProducer struct {
	Name string

	producer *kafka.Producer
}

func main() {
	lvl := new(slog.LevelVar)
	viper.SetDefault("LOG_LEVEL", "INFO")
	viper.BindEnv("LOG_LEVEL")

	lvl.Set(LevelFromString(viper.GetString("LOG_LEVEL")))

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: lvl})))
	ctx := context.Background()
	var rootCmd = &cobra.Command{
		Use:   "kfan",
		Short: "Kafka Fan-In and Fan-Out CLI Tool",
		Long:  "A tool to manage Kafka fan-in and fan-out operations via the command line.",
		Run: func(cmd *cobra.Command, args []string) {
			brokers, _ := cmd.Flags().GetString("brokers")
			group, _ := cmd.Flags().GetString("group")

			inputTopics, _ := cmd.Flags().GetString("in")
			in := strings.Split(inputTopics, ",")

			var inputs []Input
			for _, topic := range in {
				var input Input
				if topic == "std:in" {
					input = StdIn{}
					inputs = append(inputs, input)
					if len(in) > 1 {
						fmt.Println("std:in specified, ignoring other inputs")
					}
					break
				}
				parsed := Parse(topic, `^(?:(?P<brokers>[^:]+):)?(?P<topic>[^#]+)#?(?P<group>.*)$`)
				if parsed["brokers"] != "" {
					brokers = parsed["brokers"]
				}
				if parsed["group"] != "" {
					group = parsed["group"]
				}
				name := parsed["topic"]
				reset := "earliest"
				if yes, _ := cmd.Flags().GetBool("from-tail"); yes {
					reset = "latest"
				}
				input, err := NewConsumer(name, kafka.ConfigMap{
					"bootstrap.servers":  brokers,
					"group.id":           group,
					"enable.auto.commit": false,
					"auto.offset.reset":  reset,
				})
				if err != nil {
					fmt.Println(err)
					os.Exit(1)
				}
				inputs = append(inputs, input)
			}

			outputTopics, _ := cmd.Flags().GetString("out")
			out := strings.Split(outputTopics, ",")
			var outputs []Output
			for _, topic := range out {
				var output Output
				if strings.Contains(topic, "std:out") {
					outputs = append(outputs, &StdOut{
						PlusHeaders: strings.Contains(topic, "+headers"),
					})
					continue
				}
				parsed := Parse(topic, `^(?:(?P<brokers>[^:]+):)?(?P<topic>[^#]+)$`)
				if parsed["brokers"] != "" {
					brokers = parsed["brokers"]
				}
				name := parsed["topic"]
				output, err := NewProducer(name, kafka.ConfigMap{
					"bootstrap.servers": brokers,
				})
				if err != nil {
					fmt.Println(err)
					os.Exit(1)
				}
				outputs = append(outputs, output)
			}

			strategy := Broadcast
			if yes, _ := cmd.Flags().GetBool("round-robin"); yes {
				strategy = RoundRobin
			}
			if err := Execute(ctx, inputs, outputs, strategy); err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
		},
	}

	rootCmd.Flags().StringP("in", "i", "", "Comma-separated inputs -- (broker?):topic or 'std:in'")
	rootCmd.Flags().StringP("out", "o", "", "Comma-separated inputs -- (broker?):topic or 'std:out+headers'")
	rootCmd.MarkFlagRequired("in")
	rootCmd.MarkFlagRequired("out")

	rootCmd.Flags().BoolP("round-robin", "r", false, "Round-robin strategy")
	rootCmd.Flags().BoolP("broadcast", "c", true, "Broadcast strategy")
	rootCmd.MarkFlagsMutuallyExclusive("round-robin", "broadcast")

	rootCmd.Flags().BoolP("from-tail", "t", false, "Start from the tail of the topic (use 'latest' instead of 'earliest')")

	rootCmd.Flags().StringP("brokers", "b", "localhost:9092", "Kafka brokers if applicable to both 'in' and 'out'")
	viper.BindPFlag("brokers", rootCmd.Flags().Lookup("brokers"))
	viper.BindEnv("brokers", "KFAN_BROKERS")
	
	rootCmd.Flags().StringP("group", "g", "kfan-group", "Consumer group ID")
	viper.BindPFlag("group", rootCmd.Flags().Lookup("group"))
	viper.BindEnv("group", "KFAN_GROUP")

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func Execute(ctx context.Context, in []Input, out []Output, strategy Strategy) error {
	var wg sync.WaitGroup
	for _, input := range in {
		wg.Add(1)
		go func(input Input) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					slog.Error(fmt.Sprint("Recovered from panic: ", r))
				}
			}()
			switch strategy {
			case Broadcast:
				for msg := range input.PullChannel(ctx) {
					var wg sync.WaitGroup
					for _, output := range out {
						wg.Add(1)
						go func(output Output) {
							defer wg.Done()
							if err := output.Push(ctx, msg); err != nil {
								slog.Error(fmt.Sprint("Failed to push message: ", err))
							}
						}(output)
					}
					wg.Wait()
					if msg.processed != nil {
						msg.processed <- struct{}{}
					}
				}
			case RoundRobin:
				for {
					for _, output := range out {
						select {
						case msg, ok := <-input.PullChannel(ctx):
							if !ok {
								return
							}
							go func() {
								if err := output.Push(ctx, msg); err != nil {
									slog.Error(fmt.Sprint("Failed to push message: ", err))
								} else {
									msg.processed <- struct{}{}
								}
							}()
						case <-ctx.Done():
							return
						}
					}
				}
			}
		}(input)
	}
	wg.Wait()
	return nil
}

func Parse(input, regex string) map[string]string {
	re := regexp.MustCompile(regex)
	match := re.FindStringSubmatch(input)
	result := make(map[string]string)
	for i, name := range re.SubexpNames() {
		if i != 0 && name != "" {
			result[name] = match[i]
		}
	}
	return result
}

func LevelFromString(levelStr string) slog.Level {
	switch strings.ToLower(levelStr) {
	case "debug":
		return slog.LevelDebug
	case "info":
		return slog.LevelInfo
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
