// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package main

import (
	"context"
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"log"
)

func main() {
	client, err := pulsar.NewClient(pulsar.ClientOptions{URL: "pulsar://localhost:6650"})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: "topic-2",
	})
	if err != nil {
		log.Fatal(err)
	}

	defer producer.Close()

	ctx := context.Background()

	for i := 0; i < 10; i++ {
		if msgId, err := producer.Send(ctx, &pulsar.ProducerMessage{
			Payload: []byte(fmt.Sprintf("hello-%d", i)),
		}); err != nil {
			log.Fatal(err)
		} else {
			log.Println("Published message: ", msgId)
		}
	}

	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:                       "topic-2",
		SubscriptionName:            "my-sub",
		Type:                        pulsar.Exclusive,
		SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()

	lastMsg, _ := consumer.GetLastMessageID("topic-2", 0)
	log.Println("==GetLastMessageID ", lastMsg.EntryID(), lastMsg.LedgerID())

	hasNext := true
	for hasNext {
		select {
		case cm, ok := <-consumer.Chan():
			if ok {
				msg := string(cm.Payload())
				log.Println("==consumer ok", cm.Message.ID(), msg)
				consumer.Ack(cm.Message)
			} else {
				log.Println("consumer failed")
			}

			if cm.Message.ID().LedgerID() >= lastMsg.LedgerID() && cm.Message.ID().EntryID() >= lastMsg.EntryID() {
				hasNext = false
				break
			}
		}
	}

	if err := consumer.Unsubscribe(); err != nil {
		log.Fatal(err)
	}

	log.Println("consumer done... ")

}
