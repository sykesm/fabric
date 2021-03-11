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

type LedgerNotifier interface {
	CommitNotifications(done <-chan struct{}, channelName string) (<-chan *ledger.CommitNotification, error)
}

type Notifier struct {
	lock             sync.Mutex
	ledgerNotifier   LedgerNotifier
	channelNotifiers map[string]*channelNotifier
}

type Notification struct {
	BlockNumber    uint64
	TransactionID  string
	ValidationCode peer.TxValidationCode
}

func NewNotifier(ledgerNotifier LedgerNotifier) *Notifier {
	if ledgerNotifier == nil {
		panic("nil ledger notifier")
	}

	return &Notifier{
		ledgerNotifier:   ledgerNotifier,
		channelNotifiers: make(map[string]*channelNotifier),
	}
}

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
