package log_v1

import (
	"fmt"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ErrOffsetOutOfRange identifies an absolute offset absent from the log.
type ErrOffsetOutOfRange struct {
	Offset uint64
}

// GRPCStatus exposes a NotFound status with an English explanation for RPC clients.
func (e ErrOffsetOutOfRange) GRPCStatus() *status.Status {
	st := status.New(
		codes.NotFound,
		fmt.Sprintf("offset out of range: %d", e.Offset),
	)
	msg := fmt.Sprintf(
		"The requested offset is outside the log's range: %d",
		e.Offset,
	)

	d := &errdetails.LocalizedMessage{
		Locale:  "en-US",
		Message: msg,
	}
	std, err := st.WithDetails(d)
	if err != nil {
		return st
	}
	return std
}

// Error implements error using the same status text returned over gRPC.
func (e ErrOffsetOutOfRange) Error() string {
	return e.GRPCStatus().Err().Error()
}
