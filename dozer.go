// Copyright 2017 Dave Pederson.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Dozer: main module
package dozer

import (
	"errors"
	"github.com/zdavep/dozer/proto"
	_ "github.com/zdavep/dozer/proto/amqp"
	_ "github.com/zdavep/dozer/proto/kafka"
	_ "github.com/zdavep/dozer/proto/mangos"
	_ "github.com/zdavep/dozer/proto/stomp"
)

// Supported messaging protocols.
var validProto = map[string]bool{
	"amqp":   true,
	"mangos": true,
	"stomp":  true,
	"kafka":  true,
}

// Core dozer type.
type Dozer struct {
	id       uint64
	typ      string
	dest     string
	name     string
	context  []string
	protocol proto.DozerProtocol
}

// Create a new Dozer instance.
func Init(dest string) *Dozer {
	return &Dozer{dest: dest, context: make([]string, 0)}
}

// Set the use context type for credentials
func (d *Dozer) WithCredentials(user, pass string) *Dozer {
	d.context = append(d.context, user)
	d.context = append(d.context, pass)
	return d
}

// Set the protocol name field
func (d *Dozer) WithProtocol(name string) *Dozer {
	d.name = name
	return d
}

// Set the mode for producing messages (kafka support)
func (d *Dozer) Producer() *Dozer {
	d.typ = "producer"
	return d
}

// Set the mode for consuming messages (kafka support)
func (d *Dozer) Consumer() *Dozer {
	d.typ = "consumer"
	return d
}

// Connect or bind to a host and port.
func (d *Dozer) Dial(host string, port int64) error {
	if _, ok := validProto[d.name]; !ok {
		return errors.New("Unsupported protocol")
	}
	p, err := proto.LoadProtocol(d.name, d.context...)
	if err != nil {
		return err
	}
	d.protocol = p
	id, err := p.Dial(d.typ, host, port)
	if err != nil {
		return err
	}
	d.id = id
	return nil
}

// Receive messages from the lower level protocol and forward them to a channel until a quit signal fires.
func (d *Dozer) RecvLoop(messages chan []byte, quit chan bool) error {
	defer d.protocol.Close()
	if err := d.protocol.RecvFrom(d.id, d.dest, messages, quit); err != nil {
		return err
	}
	return nil
}

// Send messages to the lower level protocol from a channel until a quit signal fires.
func (d *Dozer) SendLoop(messages chan []byte, quit chan bool) error {
	defer d.protocol.Close()
	if err := d.protocol.SendTo(d.id, d.dest, messages, quit); err != nil {
		return err
	}
	return nil
}
