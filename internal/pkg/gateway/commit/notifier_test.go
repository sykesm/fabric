/*
Copyright 2021 IBM All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package commit_test

import (
	"context"
	"testing"

	"github.com/hyperledger/fabric-protos-go/peer"
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/internal/pkg/gateway/commit"
	"github.com/hyperledger/fabric/internal/pkg/gateway/commit/mock"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

//go:generate counterfeiter -o mock/notificationsupplier.go --fake-name NotificationSupplier . NotificationSupplier

func TestNotifier(t *testing.T) {
	newTestNotifier := func(commitSend <-chan *ledger.CommitNotification) *commit.Notifier {
		notificationSupplier := &mock.NotificationSupplier{}
		notificationSupplier.CommitNotificationsReturnsOnCall(0, commitSend, nil)
		notificationSupplier.CommitNotificationsReturns(nil, errors.New("unexpected call of CommitNotificationChannel"))

		return commit.NewNotifier(notificationSupplier)
	}

	t.Run("NewNotifier with nil ledger panics", func(t *testing.T) {
		f := func() {
			commit.NewNotifier(nil)
		}
		require.Panics(t, f)
	})

	t.Run("Notify", func(t *testing.T) {
		t.Run("returns error if channel does not exist", func(t *testing.T) {
			notificationSupplier := &mock.NotificationSupplier{}
			notificationSupplier.CommitNotificationsReturns(nil, errors.New("ERROR"))
			notifier := commit.NewNotifier(notificationSupplier)

			_, err := notifier.Notify(context.Background().Done(), "CHANNEL_NAME", "TX_ID")

			require.Error(t, err)
		})

		t.Run("returns notifier on successful registration", func(t *testing.T) {
			commitSend := make(chan *ledger.CommitNotification)
			notifier := newTestNotifier(commitSend)

			commitReceive, err := notifier.Notify(context.Background().Done(), "CHANNEL_NAME", "TX_ID")
			require.NoError(t, err)
			require.NotNil(t, commitReceive)
		})

		t.Run("delivers notification for matching transaction", func(t *testing.T) {
			commitSend := make(chan *ledger.CommitNotification, 1)
			notifier := newTestNotifier(commitSend)

			commitReceive, _ := notifier.Notify(context.Background().Done(), "CHANNEL_NAME", "TX_ID")
			commitSend <- &ledger.CommitNotification{
				BlockNumber: 1,
				TxIDValidationCodes: map[string]peer.TxValidationCode{
					"TX_ID": peer.TxValidationCode_MVCC_READ_CONFLICT,
				},
			}
			actual := <-commitReceive

			expected := commit.Notification{
				BlockNumber:    1,
				TransactionID:  "TX_ID",
				ValidationCode: peer.TxValidationCode_MVCC_READ_CONFLICT,
			}
			require.EqualValues(t, expected, actual)
		})

		t.Run("ignores non-matching transaction in same block", func(t *testing.T) {
			commitSend := make(chan *ledger.CommitNotification, 1)
			notifier := newTestNotifier(commitSend)

			commitReceive, _ := notifier.Notify(context.Background().Done(), "CHANNEL_NAME", "TX_ID")
			commitSend <- &ledger.CommitNotification{
				BlockNumber: 1,
				TxIDValidationCodes: map[string]peer.TxValidationCode{
					"WRONG_TX_ID": peer.TxValidationCode_VALID,
					"TX_ID":       peer.TxValidationCode_MVCC_READ_CONFLICT,
				},
			}
			actual := <-commitReceive

			expected := commit.Notification{
				BlockNumber:    1,
				TransactionID:  "TX_ID",
				ValidationCode: peer.TxValidationCode_MVCC_READ_CONFLICT,
			}
			require.EqualValues(t, expected, actual)
		})

		t.Run("ignores blocks without matching transaction", func(t *testing.T) {
			commitSend := make(chan *ledger.CommitNotification, 2)
			notifier := newTestNotifier(commitSend)

			commitReceive, _ := notifier.Notify(context.Background().Done(), "CHANNEL_NAME", "TX_ID")
			commitSend <- &ledger.CommitNotification{
				BlockNumber: 1,
				TxIDValidationCodes: map[string]peer.TxValidationCode{
					"WRONG_TX_ID": peer.TxValidationCode_VALID,
				},
			}
			commitSend <- &ledger.CommitNotification{
				BlockNumber: 2,
				TxIDValidationCodes: map[string]peer.TxValidationCode{
					"TX_ID": peer.TxValidationCode_MVCC_READ_CONFLICT,
				},
			}
			actual := <-commitReceive

			expected := commit.Notification{
				BlockNumber:    2,
				TransactionID:  "TX_ID",
				ValidationCode: peer.TxValidationCode_MVCC_READ_CONFLICT,
			}
			require.EqualValues(t, expected, actual)
		})

		t.Run("processes blocks in order", func(t *testing.T) {
			commitSend := make(chan *ledger.CommitNotification, 2)
			notifier := newTestNotifier(commitSend)

			commitReceive, _ := notifier.Notify(context.Background().Done(), "CHANNEL_NAME", "TX_ID")
			commitSend <- &ledger.CommitNotification{
				BlockNumber: 1,
				TxIDValidationCodes: map[string]peer.TxValidationCode{
					"TX_ID": peer.TxValidationCode_MVCC_READ_CONFLICT,
				},
			}
			commitSend <- &ledger.CommitNotification{
				BlockNumber: 2,
				TxIDValidationCodes: map[string]peer.TxValidationCode{
					"TX_ID": peer.TxValidationCode_MVCC_READ_CONFLICT,
				},
			}
			actual := <-commitReceive

			expected := commit.Notification{
				BlockNumber:    1,
				TransactionID:  "TX_ID",
				ValidationCode: peer.TxValidationCode_MVCC_READ_CONFLICT,
			}
			require.EqualValues(t, expected, actual)
		})

		t.Run("closes channel after notification", func(t *testing.T) {
			commitSend := make(chan *ledger.CommitNotification, 2)
			notifier := newTestNotifier(commitSend)

			commitReceive, _ := notifier.Notify(context.Background().Done(), "CHANNEL_NAME", "TX_ID")
			commitSend <- &ledger.CommitNotification{
				BlockNumber: 1,
				TxIDValidationCodes: map[string]peer.TxValidationCode{
					"TX_ID": peer.TxValidationCode_MVCC_READ_CONFLICT,
				},
			}
			commitSend <- &ledger.CommitNotification{
				BlockNumber: 2,
				TxIDValidationCodes: map[string]peer.TxValidationCode{
					"TX_ID": peer.TxValidationCode_VALID,
				},
			}
			<-commitReceive
			_, ok := <-commitReceive

			require.False(t, ok, "Expected notification channel to be closed but receive was successful")
		})

		t.Run("stops notification when done channel closed", func(t *testing.T) {
			commitSend := make(chan *ledger.CommitNotification, 1)
			notifier := newTestNotifier(commitSend)

			ctx, cancel := context.WithCancel(context.Background())
			commitReceive, _ := notifier.Notify(ctx.Done(), "CHANNEL_NAME", "TX_ID")
			cancel()
			commitSend <- &ledger.CommitNotification{
				BlockNumber: 1,
				TxIDValidationCodes: map[string]peer.TxValidationCode{
					"TX_ID": peer.TxValidationCode_MVCC_READ_CONFLICT,
				},
			}
			_, ok := <-commitReceive

			require.False(t, ok, "Expected notification channel to be closed but receive was successful")
		})

		t.Run("multiple listeners receive notifications", func(t *testing.T) {
			commitSend := make(chan *ledger.CommitNotification, 1)
			notifier := newTestNotifier(commitSend)

			commitReceive1, _ := notifier.Notify(context.Background().Done(), "CHANNEL_NAME", "TX_ID")
			commitReceive2, _ := notifier.Notify(context.Background().Done(), "CHANNEL_NAME", "TX_ID")
			commitSend <- &ledger.CommitNotification{
				BlockNumber: 1,
				TxIDValidationCodes: map[string]peer.TxValidationCode{
					"TX_ID": peer.TxValidationCode_MVCC_READ_CONFLICT,
				},
			}
			actual1 := <-commitReceive1
			actual2 := <-commitReceive2

			expected := commit.Notification{
				BlockNumber:    1,
				TransactionID:  "TX_ID",
				ValidationCode: peer.TxValidationCode_MVCC_READ_CONFLICT,
			}
			require.EqualValues(t, expected, actual1)
			require.EqualValues(t, expected, actual2)
		})

		t.Run("multiple listeners can stop listening independently", func(t *testing.T) {
			commitSend := make(chan *ledger.CommitNotification, 1)
			notifier := newTestNotifier(commitSend)

			ctx, cancel := context.WithCancel(context.Background())
			commitReceive1, _ := notifier.Notify(ctx.Done(), "CHANNEL_NAME", "TX_ID")
			commitReceive2, _ := notifier.Notify(context.Background().Done(), "CHANNEL_NAME", "TX_ID")
			cancel()
			commitSend <- &ledger.CommitNotification{
				BlockNumber: 1,
				TxIDValidationCodes: map[string]peer.TxValidationCode{
					"TX_ID": peer.TxValidationCode_MVCC_READ_CONFLICT,
				},
			}
			_, ok1 := <-commitReceive1
			_, ok2 := <-commitReceive2

			require.False(t, ok1, "Expected notification channel to be closed but receive was successful")
			require.True(t, ok2, "Expected notification channel to deliver a result but was closed")
		})
	})
}
