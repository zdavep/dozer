// Copyright 2017 Dave Pederson.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Dozer: main module
package dozer

import (
	"errors"
	"github.com/zdavep/dozer/proto"
)

// Supported messaging protocols.
var validProto = map[string]bool{
	"amqp":   true,
	"mangos": true,
	"nats":   true,
	"stomp":  true,
	"kafka":  true,
}

// Core dozer type.
type Dozer struct {
	mode         string
	dest         string
	protocolId   uint64
	protocolName string
	context      []string
	protocol     proto.DozerProtocol
}

// Create a new Dozer instance.
func Init(name string) *Dozer {
	return &Dozer{protocolName: name, context: make([]string, 0)}
}

// Set the auth credentials
func (d *Dozer) WithCredentials(user, pass string) *Dozer {
	d.context = append(d.context, user)
	d.context = append(d.context, pass)
	return d
}

// Set destination in producer mode
func (d *Dozer) Producer(dest string) *Dozer {
	d.dest = dest
	d.mode = "producer"
	return d
}

// Set destination in consumer mode
func (d *Dozer) Consumer(dest string) *Dozer {
	d.dest = dest
	d.mode = "consumer"
	return d
}

// Connect or bind to a host and port.
func (d *Dozer) Dial(host string, port int64) error {
	if _, ok := validProto[d.protocolName]; !ok {
		return errors.New("Unsupported protocol")
	}
	p, err := proto.LoadProtocol(d.protocolName, d.context...)
	if err != nil {
		return err
	}
	d.protocol = p
	id, err := p.Dial(d.mode, host, port)
	if err != nil {
		return err
	}
	d.protocolId = id
	return nil
}

// Receive messages from the lower level protocol and forward them to a channel until a quit signal fires.
func (d *Dozer) RecvLoop(messages chan []byte, quit chan bool) error {
	defer d.protocol.Close()
	if err := d.protocol.RecvFrom(d.protocolId, d.dest, messages, quit); err != nil {
		return err
	}
	return nil
}

// Send messages to the lower level protocol from a channel until a quit signal fires.
func (d *Dozer) SendLoop(messages chan []byte, quit chan bool) error {
	defer d.protocol.Close()
	if err := d.protocol.SendTo(d.protocolId, d.dest, messages, quit); err != nil {
		return err
	}
	return nil
}
