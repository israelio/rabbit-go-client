package rabbitmq

import (
	"fmt"

	"github.com/israelio/rabbit-go-client/internal/frame"
	"github.com/israelio/rabbit-go-client/internal/protocol"
)

// TxSelect puts the channel into transaction mode
func (ch *Channel) TxSelect() error {
	if ch.GetState() != ChannelStateOpen {
		return ErrChannelClosed
	}

	builder := frame.NewMethodArgsBuilder()
	method, err := ch.rpcCall(protocol.ClassTx, protocol.MethodTxSelect, builder.Bytes())
	if err != nil {
		return err
	}

	if method.MethodID != protocol.MethodTxSelectOk {
		return fmt.Errorf("unexpected response to Tx.Select: %d", method.MethodID)
	}

	ch.txMode.Store(true)
	return nil
}

// TxCommit commits the current transaction
func (ch *Channel) TxCommit() error {
	if ch.GetState() != ChannelStateOpen {
		return ErrChannelClosed
	}

	if !ch.txMode.Load() {
		return fmt.Errorf("channel not in transaction mode")
	}

	builder := frame.NewMethodArgsBuilder()
	method, err := ch.rpcCall(protocol.ClassTx, protocol.MethodTxCommit, builder.Bytes())
	if err != nil {
		return err
	}

	if method.MethodID != protocol.MethodTxCommitOk {
		return fmt.Errorf("unexpected response to Tx.Commit: %d", method.MethodID)
	}

	return nil
}

// TxRollback rolls back the current transaction
func (ch *Channel) TxRollback() error {
	if ch.GetState() != ChannelStateOpen {
		return ErrChannelClosed
	}

	if !ch.txMode.Load() {
		return fmt.Errorf("channel not in transaction mode")
	}

	builder := frame.NewMethodArgsBuilder()
	method, err := ch.rpcCall(protocol.ClassTx, protocol.MethodTxRollback, builder.Bytes())
	if err != nil {
		return err
	}

	if method.MethodID != protocol.MethodTxRollbackOk {
		return fmt.Errorf("unexpected response to Tx.Rollback: %d", method.MethodID)
	}

	return nil
}
