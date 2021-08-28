package nsq

import (
	"context"
	"errors"
	"log"
	"nsqtest/pkg/event/event"
	"time"

	"github.com/nsqio/go-nsq"
)

var (
	MessageNullErr = errors.New("message can not be empty")
)

type Message struct {
	key    string
	value  []byte
	header map[string]string
}

func (m *Message) Key() string {
	return m.key
}
func (m *Message) Value() []byte {
	return m.value
}
func (m *Message) Header() map[string]string {
	return m.header
}

func NewMessage(key string, value []byte, header map[string]string) event.Message {
	return &Message{
		key:    key,
		value:  value,
		header: header,
	}
}

type nsqSender struct {
	writer *nsq.Producer
	topic  string
	chanel string
}

func (s *nsqSender) Send(ctx context.Context, message event.Message) error {
	//没有对Context 的处理（待优化）
	//判断空串
	if len(message.Value())==0 {
		return MessageNullErr
	}
	err := s.writer.Publish(s.topic,message.Value())
	if err != nil {
		log.Println("nsq public error:",err)
		return err
	}
	return nil
}

func (s *nsqSender) Close() error {
	s.writer.Stop()
	return nil
}

func NewNsqSender(address string, topic string,chanel string) (event.Sender, error) {
	producer,err := nsq.NewProducer(address,nsq.NewConfig())
	if err != nil {
		return nil,err
	}
	return &nsqSender{writer: producer, topic: topic,chanel: chanel}, nil
}

type nsqReceiver struct {
	reader *nsq.Consumer
	topic  string
	channel string
	// 消息数
	msqCount int
	// 标识id
	nsqHandlerID string
}



func (k *nsqReceiver) Receive(ctx context.Context, handler event.Handler) error {

	return nil
}

func (k *nsqReceiver) Close() error {
	k.reader.Stop()
	return nil
}


func NewNsqReceiver(addr,topic, channel string) (event.Receiver, error) {
	return newNsqConsumer(addr,topic, channel,nsq.HandlerFunc(func(message *nsq.Message) error {
		return nil
	}))
}

func newNsqConsumer(addr,topic, channel string,handler nsq.Handler) (event.Receiver, error) {
	cfg := initConfig()

	c,err := nsq.NewConsumer(topic,channel,cfg)
	if err != nil {
		log.Println("init Consumer NewConsumer error:",err)
		return nil,err
	}
	// 添加处理回调
	c.AddHandler(handler)

	err = c.ConnectToNSQD(addr)
	if err != nil {
		log.Println("init Consumer ConnectToNSQD error:",err)
		return nil,err
	}
	return &nsqReceiver{reader: c, topic: topic,channel: channel}, nil
}

func initConfig() *nsq.Config{
	config := nsq.NewConfig()
	config.DialTimeout = time.Second * 60
	config.MsgTimeout = time.Second * 60
	config.ReadTimeout = time.Second * 60
	config.WriteTimeout = time.Second * 60
	config.HeartbeatInterval = time.Second * 10
	return config
}
