// Copyright 2017 Dave Pederson.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Dozer: nats protocol module
package nats

import (
	"errors"
	"fmt"
	"github.com/nats-io/go-nats"
	"github.com/zdavep/dozer/proto"
	"sync"
	"sync/atomic"
)

// NATS protocol type.
type DozerProtocolNats struct {
	sync.RWMutex
	conns map[uint64]*nats.Conn
}

// Id sequence
var counter uint64

// Register NATS protocol.
func init() {
	proto.Register("nats", &DozerProtocolNats{})
}

// Intialize the NATS protocol
func (p *DozerProtocolNats) Init(args ...string) error {
	p.Lock()
	p.conns = make(map[uint64]*nats.Conn)
	p.Unlock()
	return nil
}

// Connect to a NATS server
func (p *DozerProtocolNats) Dial(typ, host string, port int64) (uint64, error) {
	url := fmt.Sprintf("nats://%s:%d", host, port)
	conn, err := nats.Connect(url)
	if err != nil {
		return 0, err
	}
	id := atomic.AddUint64(&counter, 1)
	p.Lock()
	p.conns[id] = conn
	p.Unlock()
	return id, nil
}

// Receive messages and forward them to a channel until a quit signal fires.
func (p *DozerProtocolNats) RecvFrom(id uint64, dest string, messages chan []byte, quit chan bool) error {
	if dest == "" {
		return errors.New("Invalid queue name")
	}
	p.RLock()
	conn := p.conns[id]
	p.RUnlock()
	go func() {
		conn.Subscribe(dest, func(msg *nats.Msg) {
			messages <- msg.Data
		})
	}()
	<-quit
	return nil
}

// Publish messages from a channel until a quit signal fires.
func (p *DozerProtocolNats) SendTo(id uint64, dest string, messages chan []byte, quit chan bool) error {
	if dest == "" {
		return errors.New("Invalid queue/topic name")
	}
	p.RLock()
	conn := p.conns[id]
	p.RUnlock()
	for {
		select {
		case <-quit:
			return nil
		case msg := <-messages:
			conn.Publish(dest, msg)
		}
	}
}

// Close connection to NATS server
func (p *DozerProtocolNats) Close() error {
	p.Lock()
	defer p.Unlock()
	if len(p.conns) > 0 {
		for id, conn := range p.conns {
			conn.Close()
			delete(p.conns, id)
		}
	}
	return nil
}
