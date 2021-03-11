/*
Copyright 2021 IBM All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package gateway

import (
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/core/peer"
	"github.com/pkg/errors"
)

func NewPeerNotifierAdapter(p *peer.Peer) *PeerNotifierAdapter {
	if p == nil {
		panic("nil peer")
	}

	return &PeerNotifierAdapter{
		peer: p,
	}
}

type PeerNotifierAdapter struct {
	peer *peer.Peer
}

func (adapter *PeerNotifierAdapter) CommitNotifications(done <-chan struct{}, channelName string) (<-chan *ledger.CommitNotification, error) {
	channel := adapter.peer.Channel(channelName)
	if channel == nil {
		return nil, errors.Errorf("Channel does not exist: %s", channelName)
	}

	return channel.Ledger().CommitNotificationsChannel(done)
}
