// Copyright 2016 Dave Pederson.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
package main

import (
	"github.com/zdavep/dozer"
	"log"
	"os"
	"os/signal"
	"runtime"
)

// Example message handler function.
func messageHandler(id int, messages chan []byte) {
	for message := range messages {
		log.Printf("%d: Received [ %s ]\n", id, string(message))
	}
}

// Consume messages from a Mangos socket.
func main() {

	// Create a dozer Mangos socket instance
	dz := dozer.Socket("recv").WithProtocol("mangos")
	err := dz.Connect("localhost", 5555)
	if err != nil {
		log.Fatal(err)
	}

	// Helper channels
	messages, quit := make(chan []byte), make(chan bool)

	// Dedicate a majority of CPUs to message processing.
	workers := runtime.NumCPU()/2 + 1
	for i := 1; i <= workers; i++ {
		go messageHandler(i, messages)
	}

	// Start receiving messages
	log.Println("Receiving messages; hit [ctrl-c] to quit.")
	go func() {
		if err := dz.RecvLoop(messages, quit); err != nil {
			log.Println(err)
		}
	}()

	// Listen for [ctrl-c] interrupt signal
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
	<-interrupt
	signal.Stop(interrupt)
	os.Exit(0)
}
