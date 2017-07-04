// Copyright 2017 Dave Pederson.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Dozer: kafka protocol module
package kafka

import (
	"errors"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/zdavep/dozer/proto"
	"sync"
	"sync/atomic"
)

// Kafka connection type
type Conn struct {
	consumer sarama.Consumer
	producer sarama.SyncProducer
}

// Kafka protocol type.
type DozerProtocolKafka struct {
	sync.RWMutex
	connections map[uint64]Conn
}

// Id sequence
var counter uint64

// Register the kafka protocol.
func init() {
	proto.Register("kafka", &DozerProtocolKafka{})
}

// Init - intialize the kafka protocol
func (p *DozerProtocolKafka) Init(args ...string) error {
	p.Lock()
	p.connections = make(map[uint64]Conn)
	p.Unlock()
	return nil
}

// Dial - connect to a kafka broker
func (p *DozerProtocolKafka) Dial(typ, host string, port int64) (uint64, error) {
	var conn Conn
	broker := fmt.Sprintf("%s:%d", host, port)
	if typ == "consumer" {
		consumer, err := sarama.NewConsumer([]string{broker}, nil)
		if err != nil {
			return 0, err
		}
		conn = Conn{consumer, nil}
	} else if typ == "producer" {
		producer, err := sarama.NewSyncProducer([]string{broker}, nil)
		if err != nil {
			return 0, err
		}
		conn = Conn{nil, producer}
	}
	id := atomic.AddUint64(&counter, 1)
	p.Lock()
	p.connections[id] = conn
	p.Unlock()
	return id, nil
}

// RecvFrom - consume messages from a kafka topic and forward them to a channel until a quit signal fires.
func (p *DozerProtocolKafka) RecvFrom(id uint64, dest string, messages chan []byte, quit chan bool) error {
	if dest == "" {
		return errors.New("Invalid topic name")
	}
	p.RLock()
	conn := p.connections[id]
	p.RUnlock()
	consumer, err := conn.consumer.ConsumePartition(dest, 0, sarama.OffsetNewest)
	if err != nil {
		return err
	}
	go func() {
		for msg := range consumer.Messages() {
			messages <- msg.Value
		}
	}()
	<-quit
	return consumer.Close()
}

// SendTo - publish messages to a kafka topic from a channel until a quit signal fires.
func (p *DozerProtocolKafka) SendTo(id uint64, dest string, messages chan []byte, quit chan bool) error {
	if dest == "" {
		return errors.New("Invalid topic name")
	}
	p.RLock()
	conn := p.connections[id]
	p.RUnlock()
	for {
		select {
		case msg := <-messages:
			data := &sarama.ProducerMessage{Topic: dest, Value: sarama.ByteEncoder(msg)}
			_, _, err := conn.producer.SendMessage(data)
			if err != nil {
				return err
			}
		case <-quit:
			return nil
		}
	}
}

// Close - Disconnect from the kafka broker.
func (p *DozerProtocolKafka) Close() error {
	p.Lock()
	defer p.Unlock()
	if len(p.connections) > 0 {
		for id, conn := range p.connections {
			if conn.consumer != nil {
				if err := conn.consumer.Close(); err != nil {
					return err
				}
			}
			if conn.producer != nil {
				if err := conn.producer.Close(); err != nil {
					return err
				}
			}
			delete(p.connections, id)
		}
	}
	return nil
}
