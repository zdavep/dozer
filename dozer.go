// Copyright 2015 Dave Pederson.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Dozer: main module
package dozer

import (
	"errors"
	"github.com/zdavep/dozer/proto"
	_ "github.com/zdavep/dozer/proto/stomp"
)

// Core dozer type.
type Dozer struct {
	Queue     string
	protoName string
	msgTyp    string
	protocol  proto.DozerProtocol
}

// Create a new Dozer with no content type.
func New(queue string) *Dozer {
	return &Dozer{Queue: queue}
}

// Set the message type field
func (d *Dozer) WithMessageType(typ string) *Dozer {
	d.msgTyp = typ
	return d
}

// Set the protocol name field
func (d *Dozer) WithProtocol(protocolName string) *Dozer {
	d.protoName = protocolName
	return d
}

// Connect to queue using the stomp protocol
func (d *Dozer) Connect(host string, port int64) error {
	var p proto.DozerProtocol
	// TODO: Add more protocols here...
	if d.protoName == "stomp" {
		var err error
		p, err = proto.LoadProtocol("stomp", d.msgTyp, "tcp")
		if err != nil {
			return err
		}
	} else {
		return errors.New("Unsupported protocol")
	}
	if err := p.Dial(host, port); err != nil {
		return err
	}
	d.protocol = p
	return nil
}

// Receive messages from the lower level protocol and forward them to a channel
// until a quit signal fires.
func (d *Dozer) RecvLoop(messages chan []byte, quit chan bool) error {
	if err := d.protocol.Subscribe(d.Queue); err != nil {
		return err
	}
	if err := d.protocol.RecvLoop(messages, quit); err != nil {
		return err
	}
	return nil
}
