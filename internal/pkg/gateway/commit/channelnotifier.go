/*
Copyright 2021 IBM All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package commit

import (
	"sync"

	"github.com/hyperledger/fabric/core/ledger"
)

type channelLevelNotifier struct {
	lock          sync.Mutex
	commitChannel <-chan *ledger.CommitNotification
	listeners     map[string][]*channelLevelListener
}

func newChannelNotifier(commitChannel <-chan *ledger.CommitNotification) *channelLevelNotifier {
	notifier := &channelLevelNotifier{
		commitChannel: commitChannel,
		listeners:     make(map[string][]*channelLevelListener),
	}
	go notifier.run()
	return notifier
}

func (notifier *channelLevelNotifier) run() {
	for {
		blockCommit, ok := <-notifier.commitChannel
		if !ok {
			break
		}

		notifier.removeCompletedListeners()

		for transactionID, status := range blockCommit.TxIDValidationCodes {
			notification := &Notification{
				BlockNumber:    blockCommit.BlockNumber,
				TransactionID:  transactionID,
				ValidationCode: status,
			}
			notifier.notify(notification)
		}
	}
}

func (notifier *channelLevelNotifier) removeCompletedListeners() {
	notifier.lock.Lock()
	defer notifier.lock.Unlock()

	for key, listeners := range notifier.listeners {
		for i := 0; i < len(listeners); {
			if !listeners[i].isDone() {
				i++
				continue
			}

			listeners[i].close()

			lastIndex := len(listeners) - 1
			listeners[i] = listeners[lastIndex]
			listeners = listeners[:lastIndex]
		}

		if len(listeners) > 0 {
			notifier.listeners[key] = listeners
		} else {
			delete(notifier.listeners, key)
		}
	}
}

func (notifier *channelLevelNotifier) notify(notification *Notification) {
	notifier.lock.Lock()
	defer notifier.lock.Unlock()

	for _, listener := range notifier.listeners[notification.TransactionID] {
		listener.receive(notification)
		listener.close()
	}

	delete(notifier.listeners, notification.TransactionID)
}

func (notifier *channelLevelNotifier) registerListener(done <-chan struct{}, transactionID string) <-chan Notification {
	notifyChannel := make(chan Notification, 1) // avoid blocking and only expect one notification per channel
	listener := &channelLevelListener{
		done:          done,
		transactionID: transactionID,
		notifyChannel: notifyChannel,
	}

	notifier.lock.Lock()
	notifier.listeners[transactionID] = append(notifier.listeners[transactionID], listener)
	notifier.lock.Unlock()

	return notifyChannel
}

type channelLevelListener struct {
	done          <-chan struct{}
	transactionID string
	notifyChannel chan<- Notification
}

func (listener *channelLevelListener) isDone() bool {
	select {
	case <-listener.done:
		return true
	default:
		return false
	}
}

func (listener *channelLevelListener) close() {
	close(listener.notifyChannel)
}

func (listener *channelLevelListener) receive(notification *Notification) {
	listener.notifyChannel <- *notification
}
