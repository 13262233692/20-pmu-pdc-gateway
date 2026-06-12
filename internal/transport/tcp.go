package transport

import (
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/wams/pmu-pdc-gateway/internal/config"
	"github.com/wams/pmu-pdc-gateway/internal/protocol/c37118"
	"github.com/wams/pmu-pdc-gateway/pkg/pool"
)

type DataHandler func(*c37118.PMUData)

type ServerStats struct {
	ActiveConns   uint64
	TotalConns    uint64
	BytesReceived uint64
	FramesParsed  uint64
	ParseErrors   uint64
}

type TCPServer struct {
	cfg        config.TCPConfig
	parser     *c37118.Parser
	handler    DataHandler
	pmuPool    *pool.PMUDataPool
	bytePool   *pool.ByteBufferPool

	listener   net.Listener
	connWg     sync.WaitGroup
	stopCh     chan struct{}
	once       sync.Once

	stats      ServerStats
	mu         sync.Mutex
	conns      map[net.Conn]struct{}
}

func NewTCPServer(cfg config.TCPConfig, parser *c37118.Parser, handler DataHandler,
	pmuPool *pool.PMUDataPool, bytePool *pool.ByteBufferPool) *TCPServer {
	return &TCPServer{
		cfg:      cfg,
		parser:   parser,
		handler:  handler,
		pmuPool:  pmuPool,
		bytePool: bytePool,
		stopCh:   make(chan struct{}),
		conns:    make(map[net.Conn]struct{}),
	}
}

func (s *TCPServer) Start() error {
	ln, err := net.Listen("tcp", s.cfg.ListenAddr)
	if err != nil {
		return err
	}
	s.listener = ln

	go s.acceptLoop()
	return nil
}

func (s *TCPServer) Stop() {
	s.once.Do(func() {
		close(s.stopCh)
		if s.listener != nil {
			s.listener.Close()
		}
	})

	s.mu.Lock()
	for c := range s.conns {
		c.Close()
	}
	s.mu.Unlock()

	s.connWg.Wait()
}

func (s *TCPServer) acceptLoop() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.stopCh:
				return
			default:
				time.Sleep(10 * time.Millisecond)
				continue
			}
		}

		if atomic.LoadUint64(&s.stats.ActiveConns) >= uint64(s.cfg.MaxConns) {
			conn.Close()
			continue
		}

		s.mu.Lock()
		s.conns[conn] = struct{}{}
		s.mu.Unlock()
		atomic.AddUint64(&s.stats.TotalConns, 1)
		atomic.AddUint64(&s.stats.ActiveConns, 1)

		s.connWg.Add(1)
		go s.handleConn(conn)
	}
}

func (s *TCPServer) handleConn(conn net.Conn) {
	defer func() {
		conn.Close()
		s.mu.Lock()
		delete(s.conns, conn)
		s.mu.Unlock()
		atomic.AddUint64(&s.stats.ActiveConns, ^uint64(0))
		s.connWg.Done()
	}()

	scanner := c37118.NewStreamScanner(64 * 1024)
	readBuf := s.bytePool.Get(64 * 1024)
	defer s.bytePool.Put(readBuf)

	for {
		select {
		case <-s.stopCh:
			return
		default:
		}

		if s.cfg.ReadTimeout > 0 {
			conn.SetReadDeadline(time.Now().Add(s.cfg.ReadTimeout))
		}

		n, err := conn.Read(readBuf)
		if err != nil {
			return
		}

		atomic.AddUint64(&s.stats.BytesReceived, uint64(n))
		scanner.Append(readBuf[:n])

		for {
			frame, ok := scanner.Scan()
			if !ok {
				break
			}

			pmuData := s.pmuPool.Get()
			if err := s.parser.ParseDataFrameFast(frame, pmuData); err != nil {
				atomic.AddUint64(&s.stats.ParseErrors, 1)
				s.pmuPool.Put(pmuData)
				continue
			}

			atomic.AddUint64(&s.stats.FramesParsed, 1)
			s.handler(pmuData)
		}
	}
}

func (s *TCPServer) Stats() ServerStats {
	return ServerStats{
		ActiveConns:   atomic.LoadUint64(&s.stats.ActiveConns),
		TotalConns:    atomic.LoadUint64(&s.stats.TotalConns),
		BytesReceived: atomic.LoadUint64(&s.stats.BytesReceived),
		FramesParsed:  atomic.LoadUint64(&s.stats.FramesParsed),
		ParseErrors:   atomic.LoadUint64(&s.stats.ParseErrors),
	}
}
