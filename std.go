package main

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"os"
	"regexp"
	"strings"
	"sync/atomic"
)

type StdIn struct{}

func (s StdIn) PullChannel(context.Context) <-chan Message {
	pipe := make(chan Message, 100)
	go func() {
		defer close(pipe)
		re := regexp.MustCompile(`^(?:<(?P<key>[^:]+)>)?\s?(?:\[\[(?P<headers>.*?)\]\])?\s?(?P<message>.+)$`)
		stat, err := os.Stdin.Stat()
		if err != nil {
			fmt.Printf("Error checking stdin: %v\n", err)
			os.Exit(1)
		}
		isInteractive := (stat.Mode() & os.ModeCharDevice) != 0
		if isInteractive {
			fmt.Println("Message format: <key>? [[header1=value1]]? message data")
		}
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			line := scanner.Text()
			if strings.TrimSpace(line) == "" {
				continue
			}
			match := re.FindStringSubmatch(line)
			result := make(map[string]string)
			for i, name := range re.SubexpNames() {
				if i != 0 && name != "" {
					result[name] = match[i]
				}
			}
			var key []byte = nil
			if result["key"] != "" {
				key = []byte(result["key"])
			}
			var headers map[string][]byte = nil
			if result["headers"] != "" {
				pairs := strings.Split(result["headers"], ",")
				headers = make(map[string][]byte, len(pairs))
				for _, pair := range pairs {
					keyValue := strings.Split(pair, "=")
					headers[keyValue[0]] = []byte(keyValue[1])
				}
			}
			var callback func()
			if isInteractive {
				var acks int32
				callback = func() {
					atomic.AddInt32(&acks, 1)
					fmt.Printf("[std:in] ACK (%d)\n", atomic.LoadInt32(&acks))
				}
			} else {
				callback = func() {}
			}
			pipe <- Message{
				Key:     key,
				Headers: headers,
				Value:   []byte(result["message"]),
				Processed: callback,
			}
		}
		if err := scanner.Err(); err != nil {
			slog.Error(fmt.Sprintf("Error reading from stdin: %v", err))
		}
	}()
	return pipe
}

type StdOut struct {
	PlusHeaders bool
}

func (s StdOut) Push(ctx context.Context, msg Message) error {
	if s.PlusHeaders && len(msg.Headers) > 0 {
		var parts []string
		for key, value := range msg.Headers {
			parts = append(parts, fmt.Sprintf("%s=%s", key, string(value)))
		}
		fmt.Printf("[std:out] <%s> [[%s]] %s\n", msg.Key, strings.Join(parts, ","), string(msg.Value))
	} else {
		fmt.Printf("[std:out] <%s> %s\n", msg.Key, string(msg.Value))
	}
	return nil
}
