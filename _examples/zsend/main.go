// Copyright 2015 Dave Pederson.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
package main

import (
	"fmt"
	"github.com/zdavep/dozer"
	"log"
	"math/rand"
	"time"
)

// Seed the random number generator.
func init() {
	rand.Seed(time.Now().UnixNano())
}

// Alphabet variables for random string generation.
var (
	letters    = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	lettersLen = len(letters)
)

// Generate a random string with the given length.
func randString(n int64) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(lettersLen)]
	}
	return string(b)
}

// Message producer function.
func sendWorker(messages chan []byte, timeout chan bool, quit chan bool) {
	for {
		select {
		case <-timeout:
			log.Println("Timeout signal received in worker")
			quit <- true
			return
		default:
			time.Sleep(time.Duration(rand.Int63n(500)) * time.Millisecond)
			i, s := time.Now().UnixNano(), randString(1024)
			msg := fmt.Sprintf("%d: %s", i, s)
			log.Printf("Sending [ %s ]\n", msg)
			messages <- []byte(msg)
		}
	}
}

// Send messages to a ZeroMQ socket for 10 seconds.
func main() {

	// Create a dozer ZeroMQ socket instance
	dz := dozer.Socket("send").WithProtocol("zmq4")
	err := dz.Connect("*", 5555)
	if err != nil {
		log.Fatal(err)
	}
	// Helper channels
	messages, timeout, quit := make(chan []byte), make(chan bool), make(chan bool)

	// Start timer
	go func() {
		<-time.After(10 * time.Second)
		log.Println("Timeout reached")
		timeout <- true
	}()

	// Start sending messages
	go sendWorker(messages, timeout, quit)
	if err := dz.SendLoop(messages, quit); err != nil {
		log.Println(err)
	}

	// Cleanup
	close(messages)
	close(timeout)
	close(quit)
}
