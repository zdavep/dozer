// Copyright 2015 Dave Pederson.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Dozer: ZeroMQ protocol module
package zmq4

import (
	"errors"
	"fmt"
	zmq "github.com/pebbe/zmq4"
	"github.com/zdavep/dozer/proto"
	"log"
)

// ZeroMQ protocol type.
type DozerProtocolZeroMQ struct {
	socketType string
	socket     *zmq.Socket
}

// Register ZeroMQ protocol.
func init() {
	proto.Register("zmq4", &DozerProtocolZeroMQ{})
}

// Initialize the ZeroMQ protocol.
func (p *DozerProtocolZeroMQ) Init(args ...string) error {
	if len(args) < 1 {
		return errors.New("No socket type specified.")
	}
	var socket *zmq.Socket
	var err error
	socketType := args[0]
	if socketType == "send" || socketType == "push" {
		socket, err = zmq.NewSocket(zmq.PUSH)
	} else if socketType == "recv" || socketType == "pull" {
		socket, err = zmq.NewSocket(zmq.PULL)
	} else {
		err = errors.New("Unsupported socket type: " + socketType)
	}
	if err != nil {
		return err
	}
	p.socketType = socketType
	p.socket = socket
	return nil
}

// Bind/connect to a location.
func (p *DozerProtocolZeroMQ) Dial(host string, port int64) error {
	addr := fmt.Sprintf("tcp://%s:%d", host, port)
	if p.socketType == "send" || p.socketType == "push" {
		p.socket.Bind(addr)
	} else if p.socketType == "recv" || p.socketType == "pull" {
		p.socket.Connect(addr)
	} else {
		return errors.New("Unsupported socket type: " + p.socketType)
	}
	return nil
}

// Implemented only to satisfy protocol interface.
func (p *DozerProtocolZeroMQ) RecvFrom(dest string) error {
	log.Println("RecvFrom not required for ZeroMQ")
	return nil
}

// Implemented only to satisfy protocol interface.
func (p *DozerProtocolZeroMQ) SendTo(dest string) error {
	log.Println("SendTo not required for ZeroMQ")
	return nil
}

// Receive messages from a ZeroMQ socket until a quit signal fires.
func (p *DozerProtocolZeroMQ) RecvLoop(messages chan []byte, quit chan bool) error {
	defer p.Close()
	for {
		select {
		case <-quit:
			log.Println("zmq4: Quit signal received")
			return nil
		default:
			data, err := p.socket.Recv(zmq.DONTWAIT)
			if err == nil {
				messages <- []byte(data)
			}
		}
	}
}

// Send messages to a ZeroMQ socket until a quit signal fires.
func (p *DozerProtocolZeroMQ) SendLoop(messages chan []byte, quit chan bool) error {
	defer p.Close()
	for {
		select {
		case msg := <-messages:
			if _, err := p.socket.Send(string(msg), zmq.DONTWAIT); err != nil {
				log.Println(err)
			}
		case <-quit:
			log.Println("zmq4: Quit signal received")
			return nil
		}
	}
}
func (p *DozerProtocolZeroMQ) Close() error {
	return p.socket.Close()
}
