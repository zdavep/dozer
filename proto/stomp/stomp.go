// Copyright 2016 Dave Pederson.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Dozer: stomp protocol module
package stomp

import (
	"errors"
	"fmt"
	"github.com/go-stomp/stomp"
	"github.com/go-stomp/stomp/frame"
	"github.com/zdavep/dozer/proto"
	"sync"
	"sync/atomic"
)

// Stomp protocol type.
type DozerProtocolStomp struct {
	sync.RWMutex
	conns map[uint64]*stomp.Conn
}

// Id sequence
var counter uint64

// Default send options.
var options []func(*frame.Frame) error = []func(*frame.Frame) error{
	stomp.SendOpt.NoContentLength,
	stomp.SendOpt.Header("persistent", "true"),
}

// Register stomp protocol.
func init() {
	proto.Register("stomp", &DozerProtocolStomp{})
}

// Intialize the stomp protocol
func (p *DozerProtocolStomp) Init(args ...string) error {
	p.Lock()
	p.conns = make(map[uint64]*stomp.Conn)
	p.Unlock()
	return nil
}

// Connect to a stomp server
func (p *DozerProtocolStomp) Dial(typ, host string, port int64) (uint64, error) {
	bind := fmt.Sprintf("%s:%d", host, port)
	conn, err := stomp.Dial("tcp", bind)
	if err != nil {
		return 0, err
	}
	id := atomic.AddUint64(&counter, 1)
	p.Lock()
	p.conns[id] = conn
	p.Unlock()
	return id, nil
}

// Receive messages from a queue/topic and forward them to a channel until a quit signal fires.
func (p *DozerProtocolStomp) RecvFrom(id uint64, dest string, messages chan []byte, quit chan bool) error {
	if dest == "" {
		return errors.New("Invalid queue name")
	}
	p.RLock()
	conn := p.conns[id]
	p.RUnlock()
	subs, err := conn.Subscribe(dest, stomp.AckClientIndividual)
	if err != nil {
		return err
	}
	for {
		select {
		case <-quit:
			return nil
		case msg := <-subs.C:
			messages <- msg.Body
			if err := conn.Ack(msg); err != nil {
				return err
			}
		}
	}
}

// Send messages to a queue/topic from a channel until a quit signal fires.
func (p *DozerProtocolStomp) SendTo(id uint64, dest string, messages chan []byte, quit chan bool) error {
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
			if err := conn.Send(dest, "text/plain", msg, options...); err != nil {
				return err
			}
		}
	}
}

// Unsubscribe and disconnect.
func (p *DozerProtocolStomp) Close() error {
	p.Lock()
	defer p.Unlock()
	if len(p.conns) > 0 {
		for id, conn := range p.conns {
			if err := conn.Disconnect(); err != nil {
				return err
			}
			delete(p.conns, id)
		}
	}
	return nil
}
