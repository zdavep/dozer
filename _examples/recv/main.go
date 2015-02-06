// Copyright 2015 Dave Pederson.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
package main

import (
	"github.com/zdavep/dozer"
	"log"
	"time"
)

// Example message handler function.
func messageHandler(messages chan []byte, quit chan bool) {
	for {
		select {
		case msg := <-messages:
			log.Printf("Received [ %s ]\n", string(msg))
		case <-quit:
			log.Println("Quit signal received in worker")
			close(messages)
			quit <- true
			return
		}
	}
}

// Consume messages from a test queue for 10 seconds.
func main() {

	// Create a stomp dozer instance for a queue named "test"
	dz := dozer.Queue("test").WithProtocol("stomp").WithMessageType("text/plain")
	err := dz.Connect("localhost", 61613)
	if err != nil {
		log.Fatal(err)
	}

	// Helper channels
	messages, quit, timeout := make(chan []byte), make(chan bool), make(chan bool)

	// Start message handler goroutine
	go messageHandler(messages, quit)

	// Start a 10 second timer
	go func() {
		<-time.After(10 * time.Second)
		log.Println("Timeout reached")
		timeout <- true
	}()

	// Start receiving messages
	if err := dz.RecvLoop(messages, timeout); err != nil {
		log.Println(err)
	}

	// Shut down worker (closes messages channel)
	quit <- true
	<-quit

	// Cleanup
	close(timeout)
	close(quit)
}
