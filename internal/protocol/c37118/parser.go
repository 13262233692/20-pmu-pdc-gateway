package c37118

import (
	"encoding/binary"
	"errors"
	"unsafe"
)

var (
	ErrInvalidSync     = errors.New("c37118: invalid sync word")
	ErrFrameTooShort   = errors.New("c37118: frame too short")
	ErrFrameSize       = errors.New("c37118: frame size mismatch")
	ErrCRC             = errors.New("c37118: crc check failed")
)

type FrameType uint16

const (
	FrameTypeData    FrameType = FrameType(SyncDataFrame)
	FrameTypeConfig1 FrameType = FrameType(SyncConfigFrame1)
	FrameTypeConfig2 FrameType = FrameType(SyncConfigFrame2)
	FrameTypeHeader  FrameType = FrameType(SyncHeaderFrame)
	FrameTypeCommand FrameType = FrameType(SyncCommandFrame)
	FrameTypeConfig3 FrameType = FrameType(SyncConfigFrame3)
)

type ParserConfig struct {
	PhasorFormat  PhasorFormat
	FreqFormat    FreqFormat
	AnalogFormat  AnalogFormat
	PhasorCount   int
	AnalogCount   int
	DigitalCount  int
}

type Parser struct {
	cfg           ParserConfig
	phasorSize    int
	freqSize      int
	analogSize    int
	digitalBytes  int
	dataFrameSize int
}

func NewParser(cfg ParserConfig) *Parser {
	p := &Parser{cfg: cfg}

	if cfg.PhasorFormat == PhasorFormatFloat32 {
		p.phasorSize = PhasorSize32
	} else {
		p.phasorSize = PhasorSize16
	}

	if cfg.FreqFormat == FreqFormatFloat32 {
		p.freqSize = FreqSize32
	} else {
		p.freqSize = FreqSize16
	}

	if cfg.AnalogFormat == AnalogFormatFloat32 {
		p.analogSize = AnalogSize32
	} else {
		p.analogSize = AnalogSize16
	}

	p.digitalBytes = cfg.DigitalCount * DigitalSize
	p.dataFrameSize = HeaderSize + StatSize +
		cfg.PhasorCount*p.phasorSize +
		2*p.freqSize +
		cfg.AnalogCount*p.analogSize +
		p.digitalBytes +
		CRCSize

	return p
}

func (p *Parser) DataFrameSize() int {
	return p.dataFrameSize
}

func ReadSync(data []byte) (uint16, error) {
	if len(data) < SyncSize {
		return 0, ErrFrameTooShort
	}
	return binary.BigEndian.Uint16(data[0:2]), nil
}

func ReadFrameSize(data []byte) (uint16, error) {
	if len(data) < SyncSize+FrameSizeField {
		return 0, ErrFrameTooShort
	}
	return binary.BigEndian.Uint16(data[2:4]), nil
}

func ReadIDCode(data []byte) (uint16, error) {
	if len(data) < SyncSize+FrameSizeField+IDCodeSize {
		return 0, ErrFrameTooShort
	}
	return binary.BigEndian.Uint16(data[4:6]), nil
}

func DetectFrameType(data []byte) (FrameType, error) {
	sync, err := ReadSync(data)
	if err != nil {
		return 0, err
	}
	switch sync {
	case SyncDataFrame, SyncConfigFrame1, SyncConfigFrame2,
		SyncHeaderFrame, SyncCommandFrame, SyncConfigFrame3:
		return FrameType(sync), nil
	default:
		return 0, ErrInvalidSync
	}
}

func bytesToFloat32(b []byte) float32 {
	bits := binary.BigEndian.Uint32(b)
	return *(*float32)(unsafe.Pointer(&bits))
}

func (p *Parser) ParseDataFrameFast(data []byte, out *PMUData) error {
	if len(data) < p.dataFrameSize {
		return ErrFrameTooShort
	}

	sync := binary.BigEndian.Uint16(data[0:2])
	if sync != SyncDataFrame {
		return ErrInvalidSync
	}

	out.IDCode = binary.BigEndian.Uint16(data[4:6])
	out.SOC = binary.BigEndian.Uint32(data[6:10])
	frac := binary.BigEndian.Uint32(data[10:14])
	out.Fracsec = frac
	out.Timestamp = uint64(out.SOC)*1000000000 + uint64((frac&FracsecTimeBaseMask)*1000000000/0x00FFFFFF)

	offset := HeaderSize
	out.Stat = binary.BigEndian.Uint16(data[offset : offset+StatSize])
	offset += StatSize

	phasors := out.Phasors
	if p.cfg.PhasorFormat == PhasorFormatFloat32 {
		for i := 0; i < p.cfg.PhasorCount && i < len(phasors); i++ {
			phasors[i].Real = float64(bytesToFloat32(data[offset : offset+4]))
			phasors[i].Imag = float64(bytesToFloat32(data[offset+4 : offset+8]))
			offset += 8
		}
	} else {
		for i := 0; i < p.cfg.PhasorCount && i < len(phasors); i++ {
			phasors[i].Real = float64(int16(binary.BigEndian.Uint16(data[offset : offset+2])))
			phasors[i].Imag = float64(int16(binary.BigEndian.Uint16(data[offset+2 : offset+4])))
			offset += 4
		}
	}

	if p.cfg.FreqFormat == FreqFormatFloat32 {
		out.Freq = float64(bytesToFloat32(data[offset : offset+4]))
		offset += 4
		out.DFreq = float64(bytesToFloat32(data[offset : offset+4]))
		offset += 4
	} else {
		out.Freq = float64(int16(binary.BigEndian.Uint16(data[offset:offset+2]))) / 1000.0
		offset += 2
		out.DFreq = float64(int16(binary.BigEndian.Uint16(data[offset:offset+2]))) / 1000.0
		offset += 2
	}

	analogs := out.Analogs
	if p.cfg.AnalogFormat == AnalogFormatFloat32 {
		for i := 0; i < p.cfg.AnalogCount && i < len(analogs); i++ {
			analogs[i] = float64(bytesToFloat32(data[offset : offset+4]))
			offset += 4
		}
	} else {
		for i := 0; i < p.cfg.AnalogCount && i < len(analogs); i++ {
			analogs[i] = float64(int16(binary.BigEndian.Uint16(data[offset : offset+2])))
			offset += 2
		}
	}

	digitals := out.Digitals
	for i := 0; i < p.cfg.DigitalCount && i < len(digitals); i++ {
		digitals[i] = binary.BigEndian.Uint16(data[offset : offset+DigitalSize])
		offset += DigitalSize
	}

	return nil
}

func (p *Parser) ParseDataFrame(data []byte, out *PMUData) error {
	frameLen := len(data)
	if frameLen < MinDataFrameSize {
		return ErrFrameTooShort
	}

	sync := binary.BigEndian.Uint16(data[0:2])
	if sync != SyncDataFrame {
		return ErrInvalidSync
	}

	declaredSize := binary.BigEndian.Uint16(data[2:4])
	if int(declaredSize) != frameLen {
		return ErrFrameSize
	}

	if err := p.ParseDataFrameFast(data, out); err != nil {
		return err
	}

	crcOffset := frameLen - CRCSize
	out.CRC = binary.BigEndian.Uint16(data[crcOffset : crcOffset+CRCSize])
	calcCRC := CRC16CCITT(data[:crcOffset])
	if out.CRC != calcCRC {
		return ErrCRC
	}

	return nil
}

func CRC16CCITT(data []byte) uint16 {
	crc := uint16(0xFFFF)
	for _, b := range data {
		crc ^= uint16(b) << 8
		for i := 0; i < 8; i++ {
			if crc&0x8000 != 0 {
				crc = (crc << 1) ^ 0x1021
			} else {
				crc <<= 1
			}
		}
	}
	return crc
}

func FindSyncOffset(data []byte) int {
	for i := 0; i <= len(data)-SyncSize; i++ {
		sync := binary.BigEndian.Uint16(data[i : i+SyncSize])
		if sync == SyncDataFrame || sync == SyncConfigFrame1 || sync == SyncConfigFrame2 ||
			sync == SyncHeaderFrame || sync == SyncCommandFrame || sync == SyncConfigFrame3 {
			return i
		}
	}
	return -1
}

type StreamScanner struct {
	buf    []byte
	offset int
}

func NewStreamScanner(initialCap int) *StreamScanner {
	return &StreamScanner{buf: make([]byte, 0, initialCap)}
}

func (s *StreamScanner) Append(data []byte) {
	s.buf = append(s.buf, data...)
}

func (s *StreamScanner) Reset() {
	if s.offset > 0 {
		remaining := len(s.buf) - s.offset
		if remaining > 0 {
			copy(s.buf, s.buf[s.offset:])
		}
		s.buf = s.buf[:remaining]
		s.offset = 0
	}
}

func (s *StreamScanner) Scan() ([]byte, bool) {
	s.Reset()

	data := s.buf
	for len(data) >= SyncSize+FrameSizeField {
		sync := binary.BigEndian.Uint16(data[0:2])
		if isValidSync(sync) {
			frameSize := int(binary.BigEndian.Uint16(data[2:4]))
			if frameSize < MinDataFrameSize || frameSize > MaxFrameSize {
				data = data[1:]
				s.offset++
				continue
			}
			if len(data) >= frameSize {
				frame := make([]byte, frameSize)
				copy(frame, data[:frameSize])
				s.offset += frameSize
				return frame, true
			}
			return nil, false
		}
		data = data[1:]
		s.offset++
	}
	return nil, false
}

func isValidSync(sync uint16) bool {
	return sync == SyncDataFrame || sync == SyncConfigFrame1 || sync == SyncConfigFrame2 ||
		sync == SyncHeaderFrame || sync == SyncCommandFrame || sync == SyncConfigFrame3
}

func (s *StreamScanner) Buffered() int {
	return len(s.buf) - s.offset
}
