/*
Copyright 2021 IBM All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package commit

import (
	"context"
	"sync"

	"github.com/hyperledger/fabric-protos-go/peer"
	"github.com/hyperledger/fabric/core/ledger"
)

// LedgerNotifier obtains a commit notification channel for a specific ledger. It provides an abstraction of the use of
// Peer, Channel and Ledger to obtain this result, and allows mocking in unit tests.
type LedgerNotifier interface {
	CommitNotifications(done <-chan struct{}, channelName string) (<-chan *ledger.CommitNotification, error)
}

// Notifier provides notification of transaction commits.
type Notifier struct {
	lock             sync.Mutex
	ledgerNotifier   LedgerNotifier
	channelNotifiers map[string]*channelNotifier
}

// Notification of a specific transaction commit.
type Notification struct {
	BlockNumber    uint64
	TransactionID  string
	ValidationCode peer.TxValidationCode
}

// NewNotifier constructor.
func NewNotifier(ledgerNotifier LedgerNotifier) *Notifier {
	if ledgerNotifier == nil {
		panic("nil ledger notifier")
	}

	return &Notifier{
		ledgerNotifier:   ledgerNotifier,
		channelNotifiers: make(map[string]*channelNotifier),
	}
}

// Notify the caller when the named transaction commits on the named channel. The caller is only notified of commits
// occuring after registering for notifications.
func (notifier *Notifier) Notify(done <-chan struct{}, channelName string, transactionID string) (<-chan Notification, error) {
	channelNotifier, err := notifier.channelNotifier(channelName)
	if err != nil {
		return nil, err
	}

	notifyChannel := channelNotifier.RegisterListener(done, transactionID)
	return notifyChannel, nil
}

func (notifier *Notifier) channelNotifier(channelName string) (*channelNotifier, error) {
	notifier.lock.Lock()
	defer notifier.lock.Unlock()

	result := notifier.channelNotifiers[channelName]
	if result != nil {
		return result, nil
	}

	commitChannel, err := notifier.ledgerNotifier.CommitNotifications(context.Background().Done(), channelName)
	if err != nil {
		return nil, err
	}

	result = newChannelNotifier(commitChannel)
	notifier.channelNotifiers[channelName] = result

	return result, nil
}
