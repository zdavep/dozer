// Copyright 2016 Dave Pederson.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Dozer: amqp protocol module
package amqp

import (
	"errors"
	"fmt"
	"github.com/streadway/amqp"
	"github.com/zdavep/dozer/proto"
	"log"
)

// AMQP protocol type.
type DozerProtocolAmqp struct {
	Creds string
	Conn  *amqp.Connection
	Chan  *amqp.Channel
	Queue amqp.Queue
}

// Register AMQP protocol.
func init() {
	proto.Register("amqp", &DozerProtocolAmqp{})
}

// Intialize the AMQP protocol
func (p *DozerProtocolAmqp) Init(args ...string) error {
	if len(args) >= 2 {
		p.Creds = fmt.Sprintf("%s:%s", args[0], args[1])
	}
	return nil
}

// Connect to a AMQP server
func (p *DozerProtocolAmqp) Dial(host string, port int64) error {
	bind := fmt.Sprintf("amqp://%s@%s:%d", p.Creds, host, port)
	conn, err := amqp.Dial(bind)
	if err != nil {
		return err
	}
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	p.Conn = conn
	p.Chan = ch
	return nil
}

// Subscribe to the given queue using the AMQP protocol.
func (p *DozerProtocolAmqp) RecvFrom(dest string) error {
	q, err := newQueue(p, dest)
	if err != nil {
		return err
	}
	p.Queue = q
	return nil
}

// Set the name of the queue we're sending to
func (p *DozerProtocolAmqp) SendTo(dest string) error {
	q, err := newQueue(p, dest)
	if err != nil {
		return err
	}
	p.Queue = q
	return nil
}

// Create a new AMQP queue
func newQueue(p *DozerProtocolAmqp, dest string) (q amqp.Queue, err error) {
	if dest != "" {
		durable := true
		q, err = p.Chan.QueueDeclare(dest, durable, false, false, false, nil)
	} else {
		err = errors.New("Invalid queue name")
	}
	return
}

// Receive messages from a queue and forward them to a channel until a quit signal fires.
func (p *DozerProtocolAmqp) RecvLoop(messages chan []byte, quit chan bool) error {
	defer p.Close()
	queue, err := p.Chan.Consume(p.Queue.Name, "", true, false, false, false, nil)
	if err != nil {
		return err
	}
	go func() {
		for msg := range queue {
			messages <- msg.Body
		}
	}()
	<-quit
	log.Println("amqp: Quit signal received")
	return nil
}

// Send messages to a queue from a channel until a quit signal fires.
func (p *DozerProtocolAmqp) SendLoop(messages chan []byte, quit chan bool) error {
	defer p.Close()
	for {
		select {
		case msg := <-messages:
			err := p.Chan.Publish("", p.Queue.Name, false, false, amqp.Publishing{ContentType: "text/plain", Body: msg})
			if err != nil {
				return err
			}
		case <-quit:
			log.Println("amqp: Quit signal received")
			return nil
		}
	}
}

// Unsubscribe and disconnect.
func (p *DozerProtocolAmqp) Close() error {
	p.Chan.Close()
	p.Conn.Close()
	return nil
}
