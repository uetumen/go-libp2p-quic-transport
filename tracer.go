package libp2pquic

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-quic-transport/metrics"

	"github.com/lucas-clemente/quic-go/logging"
	"github.com/lucas-clemente/quic-go/qlog"
)

type quicTracer struct {
	node peer.ID
}

func newQuicTracer(peerID peer.ID) logging.Tracer {
	return &quicTracer{node: peerID}
}

var _ logging.Tracer = &quicTracer{}

func (t *quicTracer) TracerForConnection(p logging.Perspective, odcid logging.ConnectionID) logging.ConnectionTracer {
	return newConnectionTracer(p, odcid, t.node)
}
func (t *quicTracer) SentPacket(net.Addr, *logging.Header, logging.ByteCount, []logging.Frame) {}
func (t *quicTracer) DroppedPacket(net.Addr, logging.PacketType, logging.ByteCount, logging.PacketDropReason) {
}

type quicConnectionTracer struct {
	metrics.ConnectionStats
}

func newConnectionTracer(pers logging.Perspective, odcid logging.ConnectionID, node peer.ID) *quicConnectionTracer {
	t := &quicConnectionTracer{}
	t.ConnectionStats.ODCID = odcid
	t.ConnectionStats.Node = node
	t.ConnectionStats.Perspective = pers
	return t
}

func (t *quicConnectionTracer) StartedConnection(local, remote net.Addr, version logging.VersionNumber, srcConnID, destConnID logging.ConnectionID) {
	t.ConnectionStats.StartTime = time.Now()
	t.ConnectionStats.LocalAddr = local
	t.ConnectionStats.RemoteAddr = remote
	t.ConnectionStats.Version = version
}

func (t *quicConnectionTracer) ClosedConnection(r logging.CloseReason) {
	t.ConnectionStats.CloseReason = r
	t.ConnectionStats.EndTime = time.Now()
}
func (t *quicConnectionTracer) SentTransportParameters(*logging.TransportParameters)     {}
func (t *quicConnectionTracer) ReceivedTransportParameters(*logging.TransportParameters) {}
func (t *quicConnectionTracer) SentPacket(hdr *logging.ExtendedHeader, size logging.ByteCount, ack *logging.AckFrame, frames []logging.Frame) {
	t.ConnectionStats.PacketsSent++
}

func (t *quicConnectionTracer) ReceivedVersionNegotiationPacket(_ *logging.Header, v []logging.VersionNumber) {
	t.ConnectionStats.PacketsRcvd++
	t.ConnectionStats.VersionNegotiationVersions = v
}

func (t *quicConnectionTracer) ReceivedRetry(*logging.Header) {
	t.ConnectionStats.PacketsRcvd++
	t.ConnectionStats.RetryRcvd = true
}

func (t *quicConnectionTracer) ReceivedPacket(hdr *logging.ExtendedHeader, size logging.ByteCount, frames []logging.Frame) {
	t.ConnectionStats.PacketsRcvd++
}

func (t *quicConnectionTracer) BufferedPacket(logging.PacketType) {
	t.ConnectionStats.PacketsBuffered++
}

func (t *quicConnectionTracer) DroppedPacket(logging.PacketType, logging.ByteCount, logging.PacketDropReason) {
	t.ConnectionStats.PacketsDropped++
}

func (t *quicConnectionTracer) UpdatedMetrics(rttStats *logging.RTTStats, cwnd, bytesInFlight logging.ByteCount, packetsInFlight int) {
	t.ConnectionStats.LastRTT = metrics.RTTMeasurement{
		SmoothedRTT: rttStats.SmoothedRTT(),
		RTTVar:      rttStats.MeanDeviation(),
		MinRTT:      rttStats.MinRTT(),
	}
}

func (t *quicConnectionTracer) LostPacket(logging.EncryptionLevel, logging.PacketNumber, logging.PacketLossReason) {
	t.ConnectionStats.PacketsLost++
}

func (t *quicConnectionTracer) UpdatedCongestionState(logging.CongestionState) {}
func (t *quicConnectionTracer) UpdatedPTOCount(value uint32) {
	if value > 0 {
		t.ConnectionStats.PTOCount++
	}
}
func (t *quicConnectionTracer) UpdatedKeyFromTLS(l logging.EncryptionLevel, p logging.Perspective) {
	if l == logging.Encryption1RTT && p == logging.PerspectiveClient {
		t.ConnectionStats.HandshakeCompleteTime = time.Now()
		t.ConnectionStats.HandshakeRTT = t.ConnectionStats.LastRTT
	}
}
func (t *quicConnectionTracer) UpdatedKey(generation logging.KeyPhase, remote bool)                {}
func (t *quicConnectionTracer) DroppedEncryptionLevel(logging.EncryptionLevel)                     {}
func (t *quicConnectionTracer) DroppedKey(generation logging.KeyPhase)                             {}
func (t *quicConnectionTracer) SetLossTimer(logging.TimerType, logging.EncryptionLevel, time.Time) {}
func (t *quicConnectionTracer) LossTimerExpired(logging.TimerType, logging.EncryptionLevel)        {}
func (t *quicConnectionTracer) LossTimerCanceled()                                                 {}

// Close is called when the connection is closed.
func (t *quicConnectionTracer) Close() {
	if err := t.ConnectionStats.Save(); err != nil {
		log.Errorf("Saving connection statistics failed: %s", err)
	}
}

func (t *quicConnectionTracer) Debug(name, msg string) {}

var _ logging.ConnectionTracer = &quicConnectionTracer{}

var qlogTracer logging.Tracer

func init() {
	if qlogDir := os.Getenv("QLOGDIR"); len(qlogDir) > 0 {
		qlogTracer = initQlogger(qlogDir)
	}
}

func initQlogger(qlogDir string) logging.Tracer {
	return qlog.NewTracer(func(role logging.Perspective, connID []byte) io.WriteCloser {
		// create the QLOGDIR, if it doesn't exist
		if err := os.MkdirAll(qlogDir, 0o777); err != nil {
			log.Errorf("creating the QLOGDIR failed: %s", err)
			return nil
		}
		return newQlogger(qlogDir, role, connID)
	})
}

type qlogger struct {
	f        *os.File // QLOGDIR/.log_xxx.qlog.gz.swp
	filename string   // QLOGDIR/log_xxx.qlog.gz
	io.WriteCloser
}

func newQlogger(qlogDir string, role logging.Perspective, connID []byte) io.WriteCloser {
	t := time.Now().UTC().Format("2006-01-02T15-04-05.999999999UTC")
	r := "server"
	if role == logging.PerspectiveClient {
		r = "client"
	}
	finalFilename := fmt.Sprintf("%s%clog_%s_%s_%x.qlog.zst", qlogDir, os.PathSeparator, t, r, connID)
	filename := fmt.Sprintf("%s%c.log_%s_%s_%x.qlog.zst.swp", qlogDir, os.PathSeparator, t, r, connID)
	f, err := os.Create(filename)
	if err != nil {
		log.Errorf("unable to create qlog file %s: %s", filename, err)
		return nil
	}
	gz, err := zstd.NewWriter(f, zstd.WithEncoderLevel(zstd.SpeedFastest))
	if err != nil {
		log.Errorf("failed to initialize zstd: %s", err)
		return nil
	}
	return &qlogger{
		f:           f,
		filename:    finalFilename,
		WriteCloser: newBufferedWriteCloser(bufio.NewWriter(gz), gz),
	}
}

func (l *qlogger) Close() error {
	if err := l.WriteCloser.Close(); err != nil {
		return err
	}
	path := l.f.Name()
	if err := l.f.Close(); err != nil {
		return err
	}
	return os.Rename(path, l.filename)
}

type bufferedWriteCloser struct {
	*bufio.Writer
	io.Closer
}

func newBufferedWriteCloser(writer *bufio.Writer, closer io.Closer) io.WriteCloser {
	return &bufferedWriteCloser{
		Writer: writer,
		Closer: closer,
	}
}

func (h bufferedWriteCloser) Close() error {
	if err := h.Writer.Flush(); err != nil {
		return err
	}
	return h.Closer.Close()
}
