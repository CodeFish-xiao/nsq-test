package main

import (
	"context"
	"fmt"
	"nsqtest/pkg/event/event"
	"nsqtest/pkg/event/nsq"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	receiver, err := nsq.NewNsqReceiver("127.0.0.1:4151","topic_push_notify","chanel_push_notify" )
	if err != nil {
		panic(err)
	}
	receive(receiver)
	select {
	case <-sigs:
		_ = receiver.Close()
	}
}

func receive(receiver event.Receiver) {
	fmt.Println("start receiver")
	err := receiver.Receive(context.Background(), func(ctx context.Context, message event.Message) error {
		fmt.Printf("key:%s, value:%s, header:%s\n", message.Key(), message.Value(), message.Header())
		return nil
	})
	if err != nil {
		return
	}
}
