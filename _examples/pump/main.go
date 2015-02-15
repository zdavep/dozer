// Copyright 2015 Dave Pederson.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
package main

import (
	"github.com/zdavep/dozer"
	"log"
	"os"
	"os/signal"
)

// Consume messages from a test queue and forward them to a ZeroMQ socket.
func main() {

	// Create a dozer queue instance
	var err error
	queue := dozer.Queue("test").WithProtocol("stomp")
	err = queue.Connect("localhost", 61613)
	if err != nil {
		log.Fatal(err)
	}

	// Create a dozer ZeroMQ socket instance
	socket := dozer.Socket("push").WithProtocol("zmq4")
	err = socket.Connect("*", 5555) // Bind to all interfaces
	if err != nil {
		log.Fatal(err)
	}

	// Helper channels
	pipe, interrupted := make(chan []byte), make(chan bool)

	// Listen for [ctrl-c] interrupt signal
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
	go func() {
		<-interrupt
		signal.Stop(interrupt)
		interrupted <- true // Send signal to both loops
		interrupted <- true
	}()

	// Start forwarding messages to socket
	go func() {
		if err := socket.SendLoop(pipe, interrupted); err != nil {
			log.Fatal(err)
		}
	}()

	// Start receiving messages from queue
	log.Println("Pump started; enter [ctrl-c] to quit.")
	if err := queue.RecvLoop(pipe, interrupted); err != nil {
		log.Fatal(err)
	}

	// Cleanup
	close(pipe)
	close(interrupted)
}
