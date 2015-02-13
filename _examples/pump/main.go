// Copyright 2015 Dave Pederson.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
package main

import (
	"github.com/zdavep/dozer"
	"log"
	"time"
)

// Example message pump
func messagePump(input chan []byte, output chan []byte, quit chan bool) {
	for {
		select {
		case message := <-input:
			log.Printf("Forwarding message [ %s ]\n", string(message))
			output <- message
		case <-quit:
			log.Println("Quit signal received in pump")
			quit <- true
			return
		}
	}
}

// Consume messages from a test queue and forward them to a ZeroMQ socket for 20 seconds.
func main() {

	// Create a dozer queue instance
	var err error
	queue := dozer.Queue("test").WithProtocol("stomp").WithMessageType("text/plain")
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
	input, output := make(chan []byte), make(chan []byte)
	timeout1, timeout2 := make(chan bool), make(chan bool)
	quit := make(chan bool)

	// Start a 20 second timer
	go func() {
		<-time.After(20 * time.Second)
		log.Println("Timeout reached")
		timeout1 <- true
		timeout2 <- true
	}()

	// Start message pump
	go messagePump(input, output, quit)

	// Start forwarding messages to socket
	go func() {
		if err := socket.SendLoop(output, timeout2); err != nil {
			log.Fatal(err)
		}
	}()

	// Start receiving messages from queue
	if err := queue.RecvLoop(input, timeout1); err != nil {
		log.Fatal(err)
	}

	// Pump shutdown
	quit <- true
	<-quit

	// Cleanup
	close(input)
	close(output)
	close(timeout1)
	close(timeout2)
	close(quit)
}
