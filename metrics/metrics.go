package metrics

import (
	"context"
	"log"
	"net"
	"runtime/debug"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/lucas-clemente/quic-go/logging"
)

const (
	bigQueryDataset = "connections"
	bigQueryTable   = "quic"
)

const timeout = 5 * time.Second

var quicGoVersion string = "(devel)"

func init() {
	// Check validity of the bigquery schema.
	if _, err := bigquery.InferSchema(&connectionStats{}); err != nil {
		log.Fatal(err)
	}
	// determine quic-go version
	if quicGoVersion != "(devel)" { // variable set by ldflags
		return
	}
	info, ok := debug.ReadBuildInfo()
	if !ok { // no build info available. This happens when quic-go is not used as a library.
		return
	}
	for _, d := range info.Deps {
		if d.Path == "github.com/lucas-clemente/quic-go" {
			quicGoVersion = d.Version
			if d.Replace != nil {
				if len(d.Replace.Version) > 0 {
					quicGoVersion = d.Version
				} else {
					quicGoVersion += " (replaced)"
				}
			}
			break
		}
	}
}

type transportOrApplicationError struct {
	Remote       bool   `bigquery:"remote"`
	ErrorCode    int64  `bigquery:"error_code"`
	ReasonPhrase string `bigquery:"reason_phrase"`
}

type closeReason struct {
	Timeout          bigquery.NullString          `bigquery:"timeout"`
	StatelessReset   bigquery.NullBool            `bigquery:"stateless_reset"`
	TransportError   *transportOrApplicationError `bigquery:"transport_error,nullable"`
	ApplicationError *transportOrApplicationError `bigquery:"application_error,nullable"`
	ErrorMessage     bigquery.NullString          `bigquery:"error_message"` // not used yet
}

type rttMeasurement struct {
	MinRTT      float64 `bigquery:"min_rtt"`
	SmoothedRTT float64 `bigquery:"smoothed_rtt"`
	RTTVar      float64 `bigquery:"rtt_var"`
}

type connectionStats struct {
	NodeID                     string                 `bigquery:"node"`
	QuicGoVersion              string                 `bigquery:"quic_go_version"`
	IsClient                   bool                   `bigquery:"is_client"`
	StartTime                  time.Time              `bigquery:"start_time"`
	EndTime                    time.Time              `bigquery:"end_time"`
	ODCID                      string                 `bigquery:"odcid"`
	RetryRcvd                  bigquery.NullBool      `bigquery:"retry_rcvd"`
	VersionNegotiationVersions []string               `bigquery:"version_negotiation_versions"`
	HandshakeCompleteTime      bigquery.NullTimestamp `bigquery:"handshake_complete_time"`
	HandshakeRTT               *rttMeasurement        `bigquery:"handshake_rtt"`
	Version                    string                 `bigquery:"quic_version"`
	LocalAddr                  string                 `bigquery:"local_addr"`
	RemoteAddr                 string                 `bigquery:"remote_addr"`
	PacketsSent                int64                  `bigquery:"packets_sent"`
	PacketsRcvd                int64                  `bigquery:"packets_received"`
	PacketsBuffered            int64                  `bigquery:"packets_buffered"`
	PacketsDropped             int64                  `bigquery:"packets_dropped"`
	PacketsLost                int64                  `bigquery:"packets_lost"`
	LastRTT                    rttMeasurement         `bigquery:"last_rtt"`
	PTOCount                   int64                  `bigquery:"pto_count"`
	CloseReason                closeReason            `bigquery:"close_reason"`
	Qlog                       bigquery.NullString    `bigquery:"qlog"`
}

type RTTMeasurement struct {
	MinRTT, SmoothedRTT, RTTVar time.Duration
}

func toMilliSecond(d time.Duration) float64 { return float64(d.Nanoseconds() / 1e6) }

func (m *RTTMeasurement) toBigQuery() rttMeasurement {
	return rttMeasurement{
		MinRTT:      toMilliSecond(m.MinRTT),
		SmoothedRTT: toMilliSecond(m.SmoothedRTT),
		RTTVar:      toMilliSecond(m.RTTVar),
	}
}

type ConnectionStats struct {
	Node                                                                             peer.ID
	Perspective                                                                      logging.Perspective
	StartTime, EndTime, HandshakeCompleteTime                                        time.Time
	ODCID                                                                            logging.ConnectionID
	Version                                                                          logging.VersionNumber
	LocalAddr, RemoteAddr                                                            net.Addr
	RetryRcvd                                                                        bool
	VersionNegotiationVersions                                                       []logging.VersionNumber
	PacketsSent, PacketsRcvd, PacketsBuffered, PacketsDropped, PacketsLost, PTOCount int64
	LastRTT, HandshakeRTT                                                            RTTMeasurement
	CloseReason                                                                      logging.CloseReason
}

func (s *ConnectionStats) toBigQuery() *connectionStats {
	cr := closeReason{}
	if _, ok := s.CloseReason.StatelessReset(); ok {
		cr.StatelessReset = bigquery.NullBool{Bool: true, Valid: true}
	} else if timeout, ok := s.CloseReason.Timeout(); ok {
		timeoutReason := "unknown"
		switch timeout {
		case logging.TimeoutReasonHandshake:
			timeoutReason = "handshake"
		case logging.TimeoutReasonIdle:
			timeoutReason = "idle"
		}
		cr.Timeout = bigquery.NullString{StringVal: timeoutReason, Valid: true}
	} else if code, remote, ok := s.CloseReason.ApplicationError(); ok {
		cr.ApplicationError = &transportOrApplicationError{
			Remote:    remote,
			ErrorCode: int64(code),
		}
	} else if code, remote, ok := s.CloseReason.TransportError(); ok {
		cr.TransportError = &transportOrApplicationError{
			Remote:    remote,
			ErrorCode: int64(code),
		}
	}
	vnVersions := make([]string, len(s.VersionNegotiationVersions))
	for i, v := range s.VersionNegotiationVersions {
		vnVersions[i] = v.String()
	}

	handshakeRTT := s.HandshakeRTT.toBigQuery()
	return &connectionStats{
		NodeID:                     s.Node.Pretty(),
		QuicGoVersion:              quicGoVersion,
		IsClient:                   s.Perspective == logging.PerspectiveClient,
		StartTime:                  s.StartTime,
		EndTime:                    s.EndTime,
		ODCID:                      s.ODCID.String(),
		HandshakeCompleteTime:      bigquery.NullTimestamp{Timestamp: s.HandshakeCompleteTime, Valid: !s.HandshakeCompleteTime.IsZero()},
		HandshakeRTT:               &handshakeRTT,
		RetryRcvd:                  bigquery.NullBool{Bool: s.RetryRcvd, Valid: s.Perspective == logging.PerspectiveClient},
		VersionNegotiationVersions: vnVersions,
		Version:                    s.Version.String(),
		LocalAddr:                  s.LocalAddr.String(),
		RemoteAddr:                 s.RemoteAddr.String(),
		PacketsSent:                s.PacketsSent,
		PacketsRcvd:                s.PacketsRcvd,
		PacketsBuffered:            s.PacketsBuffered,
		PacketsDropped:             s.PacketsDropped,
		PacketsLost:                s.PacketsLost,
		LastRTT:                    s.LastRTT.toBigQuery(),
		CloseReason:                cr,
	}
}

func (s *ConnectionStats) Save() error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	cl, err := bigquery.NewClient(ctx, "transport-performance")
	if err != nil {
		return err
	}
	ins := cl.Dataset(bigQueryDataset).Table(bigQueryTable).Inserter()
	return ins.Put(ctx, s.toBigQuery())
}
