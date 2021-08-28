package main

import (
	"context"
	"fmt"
	"nsqtest/pkg/event/event"
	"nsqtest/pkg/event/nsq"
)

func main() {
	sender, err := nsq.NewNsqSender("127.0.0.1:4150","topic_push_notify","chanel_push_notify")
	if err != nil {
		panic(err)
	}

	for i := 0; i < 50; i++ {
		send(sender)
	}

	_ = sender.Close()
}

func send(sender event.Sender) {
	msg := nsq.NewMessage("kratos", []byte("hello world"), map[string]string{
		"user":  "kratos",
		"phone": "123456",
	})
	err := sender.Send(context.Background(), msg)
	if err != nil {
		panic(err)
	}
	fmt.Printf("key:%s, value:%s, header:%s\n", msg.Key(), msg.Value(), msg.Header())
}
