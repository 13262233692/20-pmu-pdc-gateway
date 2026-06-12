package transport

import (
	"net"
	"sync"
	"sync/atomic"

	"github.com/wams/pmu-pdc-gateway/internal/config"
	"github.com/wams/pmu-pdc-gateway/internal/protocol/c37118"
	"github.com/wams/pmu-pdc-gateway/pkg/pool"
)

type UDPServer struct {
	cfg        config.UDPConfig
	parser     *c37118.Parser
	handler    DataHandler
	pmuPool    *pool.PMUDataPool
	bytePool   *pool.ByteBufferPool

	conn       *net.UDPConn
	connWg     sync.WaitGroup
	stopCh     chan struct{}
	once       sync.Once

	stats      ServerStats
}

func NewUDPServer(cfg config.UDPConfig, parser *c37118.Parser, handler DataHandler,
	pmuPool *pool.PMUDataPool, bytePool *pool.ByteBufferPool) *UDPServer {
	return &UDPServer{
		cfg:      cfg,
		parser:   parser,
		handler:  handler,
		pmuPool:  pmuPool,
		bytePool: bytePool,
		stopCh:   make(chan struct{}),
	}
}

func (s *UDPServer) Start() error {
	addr, err := net.ResolveUDPAddr("udp", s.cfg.ListenAddr)
	if err != nil {
		return err
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return err
	}

	if s.cfg.ReadBuffer > 0 {
		conn.SetReadBuffer(s.cfg.ReadBuffer)
	}

	s.conn = conn
	s.connWg.Add(1)
	go s.readLoop()
	return nil
}

func (s *UDPServer) Stop() {
	s.once.Do(func() {
		close(s.stopCh)
		if s.conn != nil {
			s.conn.Close()
		}
	})
	s.connWg.Wait()
}

func (s *UDPServer) readLoop() {
	defer s.connWg.Done()

	readBuf := s.bytePool.Get(64 * 1024)
	defer s.bytePool.Put(readBuf)

	for {
		select {
		case <-s.stopCh:
			return
		default:
		}

		n, _, err := s.conn.ReadFromUDP(readBuf)
		if err != nil {
			select {
			case <-s.stopCh:
				return
			default:
				continue
			}
		}

		if n <= 0 {
			continue
		}

		atomic.AddUint64(&s.stats.BytesReceived, uint64(n))
		s.processDatagram(readBuf[:n])
	}
}

func (s *UDPServer) processDatagram(data []byte) {
	offset := 0
	dataLen := len(data)

	for offset < dataLen {
		if dataLen-offset < c37118.MinDataFrameSize {
			break
		}

		sync, err := c37118.ReadSync(data[offset:])
		if err != nil {
			break
		}

		if sync != c37118.SyncDataFrame &&
			sync != c37118.SyncConfigFrame1 &&
			sync != c37118.SyncConfigFrame2 &&
			sync != c37118.SyncHeaderFrame &&
			sync != c37118.SyncCommandFrame &&
			sync != c37118.SyncConfigFrame3 {
			offset++
			continue
		}

		frameSize, err := c37118.ReadFrameSize(data[offset:])
		if err != nil {
			break
		}

		if int(frameSize) < c37118.MinDataFrameSize || int(frameSize) > c37118.MaxFrameSize {
			offset++
			continue
		}

		if dataLen-offset < int(frameSize) {
			break
		}

		if sync == c37118.SyncDataFrame {
			pmuData := s.pmuPool.Get()
			if err := s.parser.ParseDataFrameFast(data[offset:offset+int(frameSize)], pmuData); err != nil {
				atomic.AddUint64(&s.stats.ParseErrors, 1)
				s.pmuPool.Put(pmuData)
			} else {
				atomic.AddUint64(&s.stats.FramesParsed, 1)
				s.handler(pmuData)
			}
		}

		offset += int(frameSize)
	}
}

func (s *UDPServer) Stats() ServerStats {
	return ServerStats{
		ActiveConns:   atomic.LoadUint64(&s.stats.ActiveConns),
		TotalConns:    atomic.LoadUint64(&s.stats.TotalConns),
		BytesReceived: atomic.LoadUint64(&s.stats.BytesReceived),
		FramesParsed:  atomic.LoadUint64(&s.stats.FramesParsed),
		ParseErrors:   atomic.LoadUint64(&s.stats.ParseErrors),
	}
}
