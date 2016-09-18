// Copyright 2016 Dave Pederson.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Dozer: amqp protocol module
package amqp

import (
	"fmt"
	"github.com/streadway/amqp"
	"github.com/zdavep/dozer/proto"
	"sync"
	"sync/atomic"
)

// AMQP Context type
type Context struct {
	Conn *amqp.Connection
	Chan *amqp.Channel
}

// AMQP protocol type.
type DozerProtocolAmqp struct {
	sync.RWMutex
	Creds    string
	contexts map[uint64]Context
}

// Id sequence
var counter uint64

// Register AMQP protocol.
func init() {
	proto.Register("amqp", &DozerProtocolAmqp{})
}

// Intialize the AMQP protocol
func (p *DozerProtocolAmqp) Init(args ...string) error {
	p.Lock()
	if len(args) >= 2 {
		p.Creds = fmt.Sprintf("%s:%s", args[0], args[1])
	}
	p.contexts = make(map[uint64]Context)
	p.Unlock()
	return nil
}

// Connect to a AMQP server
func (p *DozerProtocolAmqp) Dial(typ, host string, port int64) (uint64, error) {
	p.Lock()
	bind := fmt.Sprintf("amqp://%s@%s:%d", p.Creds, host, port)
	conn, err := amqp.Dial(bind)
	if err != nil {
		return 0, err
	}
	ch, err := conn.Channel()
	if err != nil {
		return 0, err
	}
	id := atomic.AddUint64(&counter, 1)
	p.contexts[id] = Context{conn, ch}
	p.Unlock()
	return id, nil
}

// Receive messages from a queue and forward them to a channel until a quit signal fires.
func (p *DozerProtocolAmqp) RecvFrom(id uint64, dest string, messages chan []byte, quit chan bool) error {
	p.RLock()
	ctx := p.contexts[id]
	p.RUnlock()
	durable := true
	q, err := ctx.Chan.QueueDeclare(dest, durable, false, false, false, nil)
	if err != nil {
		return err
	}
	queue, err := ctx.Chan.Consume(q.Name, "", true, false, false, false, nil)
	if err != nil {
		return err
	}
	go func() {
		for msg := range queue {
			messages <- msg.Body
		}
	}()
	<-quit
	return nil
}

// Send messages to a queue from a channel until a quit signal fires.
func (p *DozerProtocolAmqp) SendTo(id uint64, dest string, messages chan []byte, quit chan bool) error {
	p.RLock()
	ctx := p.contexts[id]
	p.RUnlock()
	durable := true
	q, err := ctx.Chan.QueueDeclare(dest, durable, false, false, false, nil)
	if err != nil {
		return err
	}
	if err != nil {
		return err
	}
	for {
		select {
		case <-quit:
			return nil
		case msg := <-messages:
			err := ctx.Chan.Publish("", q.Name, false, false, amqp.Publishing{ContentType: "text/plain", Body: msg})
			if err != nil {
				return err
			}
		}
	}
}

// Unsubscribe and disconnect.
func (p *DozerProtocolAmqp) Close() error {
	p.Lock()
	defer p.Unlock()
	if len(p.contexts) > 0 {
		for id, ctx := range p.contexts {
			if err := ctx.Chan.Close(); err != nil {
				return err
			}
			if err := ctx.Conn.Close(); err != nil {
				return err
			}
			delete(p.contexts, id)
		}
	}
	return nil
}
