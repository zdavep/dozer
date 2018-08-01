// Copyright 2017 Dave Pederson.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Dozer: Mangos protocol module
package mangos

import (
	"fmt"
	"sync"
	"sync/atomic"

	"nanomsg.org/go-mangos"
	"nanomsg.org/go-mangos/protocol/pull"
	"nanomsg.org/go-mangos/protocol/push"
	"nanomsg.org/go-mangos/transport/tcp"

	"github.com/zdavep/dozer/proto"
)

// Id sequence
var counter uint64

// Mangos protocol type.
type DozerProtocolMangos struct {
	sync.RWMutex
	sockets map[uint64]mangos.Socket
}

// Register Mangos protocol.
func init() {
	proto.Register("mangos", &DozerProtocolMangos{})
}

// Initialize the Mangos protocol.
func (p *DozerProtocolMangos) Init(args ...string) error {
	p.Lock()
	p.sockets = make(map[uint64]mangos.Socket)
	p.Unlock()
	return nil
}

// Bind/connect to a location.
func (p *DozerProtocolMangos) Dial(typ, host string, port int64) (uint64, error) {
	var socket mangos.Socket
	var err error
	addr := fmt.Sprintf("tcp://%s:%d", host, port)
	if typ == "producer" {
		socket, err = push.NewSocket()
		if err != nil {
			return 0, err
		}
		socket.AddTransport(tcp.NewTransport())
		if err = socket.Dial(addr); err != nil {
			return 0, err
		}
	} else if typ == "consumer" {
		socket, err = pull.NewSocket()
		if err != nil {
			return 0, err
		}
		socket.AddTransport(tcp.NewTransport())
		if err = socket.Listen(addr); err != nil {
			return 0, err
		}
	}
	id := atomic.AddUint64(&counter, 1)
	p.Lock()
	p.sockets[id] = socket
	p.Unlock()
	return id, nil
}

// Receive messages from a Mangos socket until a quit signal fires.
func (p *DozerProtocolMangos) RecvFrom(id uint64, dest string, messages chan []byte, quit chan bool) error {
	p.RLock()
	socket := p.sockets[id]
	p.RUnlock()
	for {
		select {
		case <-quit:
			return nil
		default:
			msg, err := socket.Recv()
			if err != nil {
				return err
			}
			messages <- msg
		}
	}
}

// Send messages to a Mangos socket until a quit signal fires.
func (p *DozerProtocolMangos) SendTo(id uint64, dest string, messages chan []byte, quit chan bool) error {
	p.RLock()
	socket := p.sockets[id]
	p.RUnlock()
	for {
		select {
		case <-quit:
			return nil
		case msg := <-messages:
			if err := socket.Send(msg); err != nil {
				return err
			}
		}
	}
}

// Close all underlying Mangos sockets.
func (p *DozerProtocolMangos) Close() error {
	p.Lock()
	defer p.Unlock()
	if len(p.sockets) > 0 {
		for id, socket := range p.sockets {
			err := socket.Close()
			if err != nil {
				return err
			}
			delete(p.sockets, id)
		}
	}
	return nil
}
