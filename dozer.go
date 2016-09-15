// Copyright 2016 Dave Pederson.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Dozer: main module
package dozer

import (
	"errors"
	"github.com/zdavep/dozer/proto"
	_ "github.com/zdavep/dozer/proto/amqp"
	_ "github.com/zdavep/dozer/proto/stomp"
	_ "github.com/zdavep/dozer/proto/zmq4"
)

// Supported messaging protocols.
var validProto = map[string]bool{
	"amqp":  true,
	"stomp": true,
	"zmq4":  true,
}

// Core dozer type.
type Dozer struct {
	dest      string
	protoName string
	context   []string
	protocol  proto.DozerProtocol
}

// Create a new Dozer queue.
func Queue(queue string) *Dozer {
	return &Dozer{dest: queue, context: make([]string, 0)}
}

// Create a new Dozer socket.
func Socket(socketType string) *Dozer {
	ctx := make([]string, 0)
	ctx = append(ctx, socketType)
	return &Dozer{context: ctx}
}

// Set the use context type for credentials
func (d *Dozer) WithCredentials(user, pass string) *Dozer {
	d.context = append(d.context, user)
	d.context = append(d.context, pass)
	return d
}

// Set the protocol name field
func (d *Dozer) WithProtocol(protocolName string) *Dozer {
	d.protoName = protocolName
	return d
}

// Syntactic sugar - calls Connect.
func (d *Dozer) Bind(host string, port int64) error {
	return d.Connect(host, port)
}

// Connect or bind to a host and port.
func (d *Dozer) Connect(host string, port int64) error {
	if _, ok := validProto[d.protoName]; !ok {
		return errors.New("Unsupported protocol")
	}
	p, err := proto.LoadProtocol(d.protoName, d.context...)
	if err != nil {
		return err
	}
	if err := p.Dial(host, port); err != nil {
		return err
	}
	d.protocol = p
	return nil
}

// Receive messages from the lower level protocol and forward them to a channel until a quit signal fires.
func (d *Dozer) RecvLoop(messages chan []byte, quit chan bool) error {
	if err := d.protocol.RecvFrom(d.dest); err != nil {
		return err
	}
	if err := d.protocol.RecvLoop(messages, quit); err != nil {
		return err
	}
	return nil
}

// Send messages to the lower level protocol from a channel until a quit signal fires.
func (d *Dozer) SendLoop(messages chan []byte, quit chan bool) error {
	if err := d.protocol.SendTo(d.dest); err != nil {
		return err
	}
	if err := d.protocol.SendLoop(messages, quit); err != nil {
		return err
	}
	return nil
}
