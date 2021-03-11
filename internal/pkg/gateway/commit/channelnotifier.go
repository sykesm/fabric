/*
Copyright 2021 IBM All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package commit

import (
	"sync"

	"github.com/hyperledger/fabric/core/ledger"
)

type channelNotifier struct {
	lock          sync.Mutex
	commitChannel <-chan *ledger.CommitNotification
	listeners     map[*channelListener]bool
}

func newChannelNotifier(commitChannel <-chan *ledger.CommitNotification) *channelNotifier {
	notifier := &channelNotifier{
		commitChannel: commitChannel,
		listeners:     make(map[*channelListener]bool),
	}
	go notifier.run()
	return notifier
}

func (channelNotifier *channelNotifier) run() {
	for {
		blockCommit, ok := <-channelNotifier.commitChannel
		if !ok {
			break
		}

		for transactionID, status := range blockCommit.TxIDValidationCodes {
			notification := &Notification{
				BlockNumber:    blockCommit.BlockNumber,
				TransactionID:  transactionID,
				ValidationCode: status,
			}
			channelNotifier.notify(notification)
		}
	}
}

func (channelNotifier *channelNotifier) notify(notification *Notification) {
	channelNotifier.lock.Lock()
	defer channelNotifier.lock.Unlock()

	for listener := range channelNotifier.listeners {
		complete := listener.Receive(notification)
		if complete {
			delete(channelNotifier.listeners, listener)
		}
	}
}

func (channelNotifier *channelNotifier) RegisterListener(done <-chan struct{}, transactionID string) <-chan Notification {
	notifyChannel := make(chan Notification, 1) // avoid blocking and only expect one notification per channel
	listener := &channelListener{
		done:          done,
		transactionID: transactionID,
		notifyChannel: notifyChannel,
	}

	channelNotifier.lock.Lock()
	channelNotifier.listeners[listener] = true
	channelNotifier.lock.Unlock()

	return notifyChannel
}

type channelListener struct {
	done          <-chan struct{}
	transactionID string
	notifyChannel chan<- Notification
}

func (listener *channelListener) Receive(notification *Notification) bool {
	select {
	case <-listener.done:
		close(listener.notifyChannel)
		return true
	default:
	}

	if notification.TransactionID != listener.transactionID {
		return false
	}

	listener.notifyChannel <- *notification
	close(listener.notifyChannel)
	return true
}
