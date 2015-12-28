/* Copyright 2015 LinkedIn Corp. Licensed under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package main

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"github.com/Shopify/sarama"
	log "github.com/cihub/seelog"
	"io/ioutil"
	"net/http"
	"strconv"
	"sync"
	"time"
)

type KafkaClient struct {
	app                *ApplicationContext
	cluster            string
	client             sarama.Client
	masterConsumer     sarama.Consumer
	partitionConsumers []sarama.PartitionConsumer
	requestChannel     chan *BrokerTopicRequest
	messageChannel     chan *sarama.ConsumerMessage
	errorChannel       chan *sarama.ConsumerError
	wgFanIn            sync.WaitGroup
	wgProcessor        sync.WaitGroup
	topicMap           map[string]int
	topicMapLock       sync.RWMutex
	brokerOffsetTicker *time.Ticker
	pusherTicker       *time.Ticker
}

type BrokerTopicRequest struct {
	Result chan int
	Topic  string
}

func NewKafkaClient(app *ApplicationContext, cluster string) (*KafkaClient, error) {
	// Set up sarama client
	clientConfig := sarama.NewConfig()
	clientConfig.ClientID = app.Config.General.ClientID
	sclient, err := sarama.NewClient(app.Config.Kafka[cluster].Brokers, clientConfig)
	if err != nil {
		return nil, err
	}

	// Create sarama master consumer
	master, err := sarama.NewConsumerFromClient(sclient)
	if err != nil {
		sclient.Close()
		return nil, err
	}

	client := &KafkaClient{
		app:            app,
		cluster:        cluster,
		client:         sclient,
		masterConsumer: master,
		requestChannel: make(chan *BrokerTopicRequest),
		messageChannel: make(chan *sarama.ConsumerMessage),
		errorChannel:   make(chan *sarama.ConsumerError),
		wgFanIn:        sync.WaitGroup{},
		wgProcessor:    sync.WaitGroup{},
		topicMap:       make(map[string]int),
		topicMapLock:   sync.RWMutex{},
	}

	if client.app.Config.Pusher.PusherSwitcher {
		//client.getMsgFromPusher()
		client.pusherTicker = time.NewTicker(time.Duration(client.app.Config.Pusher.PusherTimeout) * time.Second)
		go func() {
			for _ = range client.pusherTicker.C {
				client.getMsgFromPusher()
			}
		}()
	}

	// Start goroutine to handle topic metadata requests. Do this first because the getOffsets call needs this working
	client.RefreshTopicMap()
	go func() {
		for r := range client.requestChannel {
			client.getPartitionCount(r)
		}
	}()

	// Now get the first set of offsets and start a goroutine to continually check them
	client.getOffsets()
	client.brokerOffsetTicker = time.NewTicker(time.Duration(client.app.Config.Tickers.BrokerOffsets) * time.Second)
	go func() {
		for _ = range client.brokerOffsetTicker.C {
			client.getOffsets()
		}
	}()

	return client, nil
}

func (client *KafkaClient) Stop() {
	// We don't really need to do a safe stop, because we're not maintaining offsets. But we'll do it anyways
	for _, pconsumer := range client.partitionConsumers {
		pconsumer.AsyncClose()
	}

	// Wait for the Messages and Errors channel to be fully drained.
	client.wgFanIn.Wait()
	close(client.errorChannel)
	close(client.messageChannel)
	client.wgProcessor.Wait()

	// Stop the offset checker and the topic metdata refresh and request channel
	client.brokerOffsetTicker.Stop()
	client.pusherTicker.Stop()
	close(client.requestChannel)
}

// Send the offset on the specified channel, but wait no more than maxTime seconds to do so
func timeoutSendOffset(offsetChannel chan *PartitionOffset, offset *PartitionOffset, maxTime int) {
	timeout := time.After(time.Duration(maxTime) * time.Second)
	select {
	case offsetChannel <- offset:
	case <-timeout:
	}
}

// This function performs massively parallel OffsetRequests, which is better than Sarama's internal implementation,
// which does one at a time. Several orders of magnitude faster.
func (client *KafkaClient) getOffsets() error {
	// Start with refreshing the topic list
	client.RefreshTopicMap()

	requests := make(map[int32]*sarama.OffsetRequest)
	brokers := make(map[int32]*sarama.Broker)

	client.topicMapLock.RLock()

	// Generate an OffsetRequest for each topic:partition and bucket it to the leader broker
	for topic, partitions := range client.topicMap {
		for i := 0; i < partitions; i++ {
			broker, err := client.client.Leader(topic, int32(i))
			if err != nil {
				client.topicMapLock.RUnlock()
				return err
			}
			if _, ok := requests[broker.ID()]; !ok {
				requests[broker.ID()] = &sarama.OffsetRequest{}
			}
			brokers[broker.ID()] = broker
			requests[broker.ID()].AddBlock(topic, int32(i), sarama.OffsetNewest, 1)
		}
	}

	// Send out the OffsetRequest to each broker for all the partitions it is leader for
	// The results go to the offset storage module
	var wg sync.WaitGroup

	getBrokerOffsets := func(brokerID int32, request *sarama.OffsetRequest) {
		defer wg.Done()
		response, err := brokers[brokerID].GetAvailableOffsets(request)
		if err != nil {
			log.Errorf("Cannot fetch offsets from broker %v: %v", brokerID, err)
			return
		}
		ts := time.Now().Unix() * 1000
		for topic, partitions := range response.Blocks {
			for partition, offsetResponse := range partitions {
				if offsetResponse.Err != sarama.ErrNoError {
					log.Warnf("Error in OffsetResponse for %s:%v from broker %v: %s", topic, partition, brokerID, offsetResponse.Err.Error())
					continue
				}
				offset := &PartitionOffset{
					Cluster:             client.cluster,
					Topic:               topic,
					Partition:           partition,
					Offset:              offsetResponse.Offsets[0],
					Timestamp:           ts,
					TopicPartitionCount: client.topicMap[topic],
				}
				timeoutSendOffset(client.app.Storage.offsetChannel, offset, 1)
			}
		}
	}

	for brokerID, request := range requests {
		wg.Add(1)
		go getBrokerOffsets(brokerID, request)
	}

	wg.Wait()
	client.topicMapLock.RUnlock()
	return nil
}

func (client *KafkaClient) RefreshTopicMap() {
	client.topicMapLock.Lock()
	topics, _ := client.client.Topics()
	for _, topic := range topics {
		partitions, _ := client.client.Partitions(topic)
		client.topicMap[topic] = len(partitions)
	}
	client.topicMapLock.Unlock()
}

func (client *KafkaClient) getPartitionCount(r *BrokerTopicRequest) {
	client.topicMapLock.RLock()
	if partitions, ok := client.topicMap[r.Topic]; ok {
		r.Result <- partitions
	} else {
		r.Result <- -1
	}
	client.topicMapLock.RUnlock()
}

func readString(buf *bytes.Buffer) (string, error) {
	var strlen uint16
	err := binary.Read(buf, binary.BigEndian, &strlen)
	if err != nil {
		return "", err
	}
	strbytes := make([]byte, strlen)
	n, err := buf.Read(strbytes)
	if (err != nil) || (n != int(strlen)) {
		return "", errors.New("string underflow")
	}
	return string(strbytes), nil
}

type RetMsg struct {
	Data   map[string]map[string]map[string]*response
	Errmsg string
	Errno  int
}

type response struct {
	CurrRecordOpTime int64  `json:CurrRecordOpTime`
	LogId            string `json:LogId`
	Offset           int64  `json:Offset`
}

func (client *KafkaClient) getMsgFromPusher() {
	var pusherDataMap map[string]map[string]map[string]*response
	var err error
	var temp int64

	for _, pusherurl := range client.app.Config.Pusher.PusherUrl {
		if pusherDataMap, err = getPusherData(pusherurl, client.app.Config.Pusher.PusherTimeout); err != nil {
			continue
		}

		for group, groupInfo := range pusherDataMap {
			for topic, topicInfo := range groupInfo {
				for partition, offset := range topicInfo {
					if temp, err = strconv.ParseInt(partition, 10, 32); err != nil {
						log.Errorf("parse data err, group:%v, topic:%v, partition:%v\n", getGroupNameByUrl(group), topic, temp)
						continue
					}

					partitionOffset := &PartitionOffset{
						Cluster:   client.cluster,
						Topic:     topic,
						Partition: int32(temp),
						Group:     getGroupNameByUrl(group),
						Timestamp: int64(offset.CurrRecordOpTime),
						Offset:    int64(offset.Offset),
					}
					timeoutSendOffset(client.app.Storage.offsetChannel, partitionOffset, 1)
					log.Infof("tracker data => cluster:%v, topic:%v, partition:%v, group:%v, time:%v, offset:%v, data:%v\n", client.cluster, topic, partition, getGroupNameByUrl(group), offset.CurrRecordOpTime, offset.Offset, offset)
				}
			}
		}
	}
	return
}

func getPusherData(url string, timeout int) (map[string]map[string]map[string]*response, error) {
	var msg *RetMsg
	var err error
	client := http.Client{
		Timeout: time.Duration(timeout) * time.Second,
	}
	resp, err := client.Get(url)
	if err != nil { // add double check
		resp, err = client.Get(url)
		if err != nil {
			return nil, err
		}
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal([]byte(body), &msg)
	if err != nil {
		return nil, err
	}

	if msg.Errno != 0 {
		return nil, err
	}
	return msg.Data, nil
}

func getGroupNameByUrl(url string) string {
	m := md5.New()
	m.Write([]byte(url))
	s := hex.EncodeToString(m.Sum(nil))
	return s
}
