package sfu

import (
	"errors"
	"github.com/gorilla/websocket"
	"io"
	"net"
	"os"
	"testing"
)

func TestSignalingCloseReasonIsBounded(t *testing.T) {
	cases := []struct {
		err  error
		want string
	}{
		{&websocket.CloseError{Code: 1000, Text: "private client text"}, "websocket-1000"},
		{&net.OpError{Op: "read", Err: os.ErrDeadlineExceeded}, "timeout"},
		{net.ErrClosed, "socket-closed"}, {io.EOF, "eof"}, {io.ErrUnexpectedEOF, "eof"},
		{errors.New("private payload"), "transport-or-protocol-error"},
	}
	for _, tc := range cases {
		if got := signalingCloseReason(tc.err); got != tc.want {
			t.Errorf("got %q want %q", got, tc.want)
		}
	}
}
