package sfu

import (
	"testing"
	"time"

	"github.com/pion/interceptor"
	"github.com/pion/rtcp"
)

func TestBWEStateUsesFreshFeedbackOnly(t *testing.T) {
	factory := newBWEFactory()
	created, err := factory.NewInterceptor("pc-1")
	if err != nil {
		t.Fatal(err)
	}
	state := factory.state("pc-1")
	if state == nil {
		t.Fatal("factory did not register the estimator")
	}
	now := time.Now()
	if _, ok := state.target(now); ok {
		t.Fatal("a fresh transport must not claim a bandwidth estimate")
	}

	state.noteFeedback()
	target, ok := state.target(now)
	if !ok || target != bweInitialBitrate {
		t.Fatalf("initial target = %d, ok = %t; want %d", target, ok, bweInitialBitrate)
	}
	if _, ok := state.target(now.Add(bweFeedbackTTL + time.Second)); ok {
		t.Fatal("stale feedback must disable the estimate")
	}

	if err := created.Close(); err != nil {
		t.Fatal(err)
	}
	if factory.state("pc-1") != nil {
		t.Fatal("closing the interceptor must remove its state")
	}
}

func TestBWEInterceptorCountsTransportWideFeedback(t *testing.T) {
	factory := newBWEFactory()
	created, err := factory.NewInterceptor("pc-2")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = created.Close() }()

	packet := &rtcp.TransportLayerCC{
		SenderSSRC:         1,
		MediaSSRC:          2,
		BaseSequenceNumber: 0,
		PacketStatusCount:  1,
		ReferenceTime:      0,
		PacketChunks: []rtcp.PacketStatusChunk{&rtcp.RunLengthChunk{
			Type:               rtcp.TypeTCCRunLengthChunk,
			PacketStatusSymbol: rtcp.TypeTCCPacketReceivedSmallDelta,
			RunLength:          1,
		}},
		RecvDeltas: []*rtcp.RecvDelta{{Type: rtcp.TypeTCCPacketReceivedSmallDelta, Delta: 1000}},
	}
	packet.Header = rtcp.Header{
		Count:  rtcp.FormatTCC,
		Type:   rtcp.TypeTransportSpecificFeedback,
		Length: uint16(packet.MarshalSize()/4 - 1),
	}
	raw, err := rtcp.Marshal([]rtcp.Packet{packet})
	if err != nil {
		t.Fatal(err)
	}
	if packets, err := rtcp.Unmarshal(raw); err != nil || len(packets) != 1 {
		t.Fatalf("test TWCC packet is not decodable: %v, %d packets", err, len(packets))
	}
	reader := created.BindRTCPReader(interceptor.RTCPReaderFunc(func(buffer []byte, attributes interceptor.Attributes) (int, interceptor.Attributes, error) {
		copy(buffer, raw)
		return len(raw), attributes, nil
	}))
	buffer := make([]byte, 1500)
	if _, _, err := reader.Read(buffer, nil); err != nil {
		t.Fatal(err)
	}

	state := factory.state("pc-2")
	if state == nil {
		t.Fatal("factory lost the estimator state")
	}
	if state.twccCount.Load() != 1 {
		t.Fatalf("TWCC count = %d, want 1", state.twccCount.Load())
	}
	if state.lastFeedback.Load() == 0 {
		t.Fatal("feedback timestamp was not recorded")
	}
}
