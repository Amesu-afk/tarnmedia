package sfu

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/pion/interceptor"
	"github.com/pion/interceptor/pkg/gcc"
	"github.com/pion/rtcp"
)

// The downlink estimate comes from Google Congestion Control: transport-wide CC
// feedback from the subscriber is richer than REMB (and Chrome often sends no
// REMB at all). The estimator is per PeerConnection, because the feedback
// describes one transport shared by every track of that subscriber.
const (
	bweInitialBitrate = 800_000
	bweMinBitrate     = 100_000
	bweMaxBitrate     = 8_000_000
	// Without fresh feedback the estimate is stale and layer selection falls
	// back to receiver reports and queue pressure.
	bweFeedbackTTL = 3 * time.Second
	bweRefresh     = 500 * time.Millisecond
)

type bweState struct {
	id  string
	bwe *gcc.SendSideBWE

	lastFeedback atomic.Int64
	twccCount    atomic.Uint64

	mu       sync.Mutex
	cachedAt time.Time
	cached   uint32
	cachedOK bool
}

func (b *bweState) noteFeedback() {
	b.lastFeedback.Store(time.Now().UnixNano())
	b.twccCount.Add(1)
}

// target returns the current estimate and whether fresh feedback backs it.
// The estimate is cached briefly: it is read per forwarded packet.
func (b *bweState) target(now time.Time) (uint32, bool) {
	last := b.lastFeedback.Load()
	if last == 0 || now.Sub(time.Unix(0, last)) > bweFeedbackTTL {
		b.mu.Lock()
		b.cachedOK = false
		b.mu.Unlock()
		return 0, false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.cachedOK || now.Sub(b.cachedAt) >= bweRefresh {
		target := b.bwe.GetTargetBitrate()
		if target > 0 {
			b.cached = uint32(target)
			b.cachedOK = true
			b.cachedAt = now
		}
	}
	return b.cached, b.cachedOK
}

// bweFactory creates one estimator per PeerConnection. Pion calls the factory
// with the PeerConnection ID while constructing it, so the joining peer can
// look its estimator up by pc.ID().
type bweFactory struct {
	mu     sync.Mutex
	states map[string]*bweState
}

func newBWEFactory() *bweFactory {
	return &bweFactory{states: make(map[string]*bweState)}
}

func (f *bweFactory) NewInterceptor(id string) (interceptor.Interceptor, error) {
	bwe, err := gcc.NewSendSideBWE(
		gcc.SendSideBWEPacer(gcc.NewNoOpPacer()),
		gcc.SendSideBWEInitialBitrate(bweInitialBitrate),
		gcc.SendSideBWEMinBitrate(bweMinBitrate),
		gcc.SendSideBWEMaxBitrate(bweMaxBitrate),
	)
	if err != nil {
		return nil, err
	}
	state := &bweState{id: id, bwe: bwe}
	f.mu.Lock()
	f.states[id] = state
	f.mu.Unlock()
	return &bweInterceptor{factory: f, state: state}, nil
}

func (f *bweFactory) state(id string) *bweState {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.states[id]
}

func (f *bweFactory) snapshot(now time.Time) (active int, minTarget, maxTarget uint32, feedback uint64) {
	f.mu.Lock()
	states := make([]*bweState, 0, len(f.states))
	for _, state := range f.states {
		states = append(states, state)
	}
	f.mu.Unlock()
	for _, state := range states {
		feedback += state.twccCount.Load()
		if target, ok := state.target(now); ok {
			active++
			if minTarget == 0 || target < minTarget {
				minTarget = target
			}
			if target > maxTarget {
				maxTarget = target
			}
		}
	}
	return active, minTarget, maxTarget, feedback
}

// bweInterceptor records outgoing packets in the estimator and feeds it the
// subscriber's TWCC feedback. The transport-wide sequence number itself is
// added by twcc.HeaderExtensionInterceptor, which must wrap this one.
type bweInterceptor struct {
	interceptor.NoOp
	factory *bweFactory
	state   *bweState
}

func (i *bweInterceptor) BindLocalStream(info *interceptor.StreamInfo, writer interceptor.RTPWriter) interceptor.RTPWriter {
	return i.state.bwe.AddStream(info, writer)
}

func (i *bweInterceptor) BindRTCPReader(reader interceptor.RTCPReader) interceptor.RTCPReader {
	return interceptor.RTCPReaderFunc(func(buffer []byte, attributes interceptor.Attributes) (int, interceptor.Attributes, error) {
		n, attrs, err := reader.Read(buffer, attributes)
		if err != nil {
			return n, attrs, err
		}
		packets, err := rtcp.Unmarshal(buffer[:n])
		if err != nil {
			return n, attrs, nil
		}
		for _, packet := range packets {
			if _, ok := packet.(*rtcp.TransportLayerCC); ok {
				i.state.noteFeedback()
			}
		}
		// The estimator ignores reports it does not understand; an error here
		// must not break the RTCP path for the rest of the SFU.
		_ = i.state.bwe.WriteRTCP(packets, attrs)
		return n, attrs, nil
	})
}

func (i *bweInterceptor) Close() error {
	i.factory.mu.Lock()
	delete(i.factory.states, i.state.id)
	i.factory.mu.Unlock()
	return i.state.bwe.Close()
}
