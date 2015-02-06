// Copyright 2015 Dave Pederson.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Dozer: stomp protocol module
package stomp

import (
	"errors"
	"fmt"
	stmp "github.com/go-stomp/stomp"
	"github.com/zdavep/dozer/proto"
	"log"
)

// Stomp protocol type.
type DozerProtocolStomp struct {
	queue   string
	network string
	msgType string
	conn    *stmp.Conn
	subs    *stmp.Subscription
}

// Register stomp protocol.
func init() {
	proto.Register("stomp", &DozerProtocolStomp{})
}

// Intialize the stomp protocol
func (p *DozerProtocolStomp) Init(args ...string) error {
	argLen := len(args)
	if argLen == 0 {
		return errors.New("Missing queue/topic name")
	}
	if argLen >= 1 {
		p.queue = args[0]
	}
	if argLen >= 2 {
		p.msgType = args[1]
	} else {
		p.msgType = "text/plain"
	}
	if argLen >= 3 {
		p.network = args[2]
	} else {
		p.network = "tcp"
	}
	return nil
}

// Connect to a stomp server
func (p *DozerProtocolStomp) Dial(host string, port int64) error {
	bind := fmt.Sprintf("%s:%d", host, port)
	conn, err := stmp.Dial(p.network, bind, stmp.Options{})
	if err != nil {
		return err
	}
	p.conn = conn
	return nil
}

// Subscribe to the given queue or topic using the stomp protocol.
func (p *DozerProtocolStomp) Subscribe() error {
	sub, err := p.conn.Subscribe(p.queue, stmp.AckClientIndividual)
	if err != nil {
		p.conn.Disconnect()
		return err
	}
	p.subs = sub
	return nil
}

// Receive messages from a queue/topic and forward them to a channel until a quit signal fires.
func (p *DozerProtocolStomp) RecvLoop(messages chan []byte, quit chan bool) error {
	for {
		select {
		case msg := <-p.subs.C:
			messages <- msg.Body
			if err := p.conn.Ack(msg); err != nil {
				p.subs.Unsubscribe()
				p.conn.Disconnect()
				return err
			}
		case <-quit:
			log.Println("Quit signal received")
			p.subs.Unsubscribe()
			p.conn.Disconnect()
			return nil
		}
	}
}

// Send messages to a queue/topic from a channel until a quit signal fires.
func (p *DozerProtocolStomp) SendLoop(messages chan []byte, quit chan bool) error {
	for {
		select {
		case msg := <-messages:
			if err := p.conn.Send(p.queue, p.msgType, msg, nil); err != nil {
				p.subs.Unsubscribe()
				p.conn.Disconnect()
				return err
			}
		case <-quit:
			log.Println("Quit signal received")
			p.subs.Unsubscribe()
			p.conn.Disconnect()
			return nil
		}
	}
}
