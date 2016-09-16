// Copyright 2016 Dave Pederson.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Dozer: Mangos protocol module
package mangos

import (
	"errors"
	"fmt"
	"github.com/go-mangos/mangos"
	"github.com/go-mangos/mangos/protocol/pull"
	"github.com/go-mangos/mangos/protocol/push"
	"github.com/go-mangos/mangos/transport/tcp"
	"github.com/zdavep/dozer/proto"
	"log"
)

// Mangos protocol type.
type DozerProtocolMangos struct {
	socketType string
	socket     mangos.Socket
}

// Register Mangos protocol.
func init() {
	proto.Register("mangos", &DozerProtocolMangos{})
}

// Initialize the Mangos protocol.
func (p *DozerProtocolMangos) Init(args ...string) error {
	if len(args) < 1 {
		return errors.New("No socket type specified.")
	}
	var socket mangos.Socket
	var err error
	socketType := args[0]
	if socketType == "send" || socketType == "push" {
		socket, err = push.NewSocket()
		if err != nil {
			return err
		}
	} else if socketType == "recv" || socketType == "pull" {
		socket, err = pull.NewSocket()
		if err != nil {
			return err
		}
	} else {
		err = errors.New("Unsupported socket type: " + socketType)
	}
	if err != nil {
		return err
	}
	socket.AddTransport(tcp.NewTransport())
	p.socketType = socketType
	p.socket = socket
	return nil

}

// Bind/connect to a location.
func (p *DozerProtocolMangos) Dial(host string, port int64) error {
	addr := fmt.Sprintf("tcp://%s:%d", host, port)
	if p.socketType == "send" || p.socketType == "push" {
		if err := p.socket.Dial(addr); err != nil {
			return err
		}
	} else if p.socketType == "recv" || p.socketType == "pull" {
		if err := p.socket.Listen(addr); err != nil {
			return err
		}
	} else {
		return errors.New("Unsupported socket type: " + p.socketType)
	}
	return nil
}

// Implemented only to satisfy protocol interface.
func (p *DozerProtocolMangos) RecvFrom(dest string) error {
	log.Println("RecvFrom not required for Mangos")
	return nil
}

// Implemented only to satisfy protocol interface.
func (p *DozerProtocolMangos) SendTo(dest string) error {
	log.Println("SendTo not required for Mangos")
	return nil
}

// Receive messages from a Mangos socket until a quit signal fires.
func (p *DozerProtocolMangos) RecvLoop(messages chan []byte, quit chan bool) error {
	defer p.Close()
	for {
		select {
		case <-quit:
			log.Println("mangos: Quit signal received")
			return nil
		default:
			msg, err := p.socket.Recv()
			if err != nil {
				return err
			}
			messages <- msg
		}
	}
}

// Send messages to a Mangos socket until a quit signal fires.
func (p *DozerProtocolMangos) SendLoop(messages chan []byte, quit chan bool) error {
	defer p.Close()
	for {
		select {
		case msg := <-messages:
			if err := p.socket.Send(msg); err != nil {
				return err
			}
		case <-quit:
			log.Println("mangos: Quit signal received")
			return nil
		}
	}
}

// Close the underlying Mangos socket.
func (p *DozerProtocolMangos) Close() error {
	p.socket.Close()
	return nil
}
