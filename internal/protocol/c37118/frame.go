package c37118

import "math"

const (
	SyncDataFrame    uint16 = 0xAA01
	SyncConfigFrame1 uint16 = 0xAA02
	SyncConfigFrame2 uint16 = 0xAA03
	SyncHeaderFrame  uint16 = 0xAA04
	SyncCommandFrame uint16 = 0xAA05
	SyncConfigFrame3 uint16 = 0xAA06

	HeaderSize       = 16
	SyncSize         = 2
	FrameSizeField   = 2
	IDCodeSize       = 2
	SOCSize          = 4
	FracsecSize      = 4
	StatSize         = 2
	FreqSize16       = 2
	FreqSize32       = 4
	PhasorSize16     = 4
	PhasorSize32     = 8
	AnalogSize16     = 2
	AnalogSize32     = 4
	DigitalSize      = 2
	CRCSize          = 2

	MinDataFrameSize = HeaderSize + StatSize + CRCSize
	MaxFrameSize     = 65535

	FracsecTimeBaseMask uint32 = 0x00FFFFFF
	FracsecQualityMask  uint32 = 0xFF000000
	FracsecQualityShift uint8  = 24
)

type PhasorFormat uint8

const (
	PhasorFormatInt16 PhasorFormat = iota
	PhasorFormatFloat32
)

type FreqFormat uint8

const (
	FreqFormatInt16 FreqFormat = iota
	FreqFormatFloat32
)

type AnalogFormat uint8

const (
	AnalogFormatInt16 AnalogFormat = iota
	AnalogFormatFloat32
)

type Phasor struct {
	Real float64
	Imag float64
}

type PMUData struct {
	IDCode     uint16
	Timestamp  uint64
	SOC        uint32
	Fracsec    uint32
	Stat       uint16
	Freq       float64
	DFreq      float64
	Phasors    []Phasor
	Analogs    []float64
	Digitals   []uint16
	CRC        uint16
}

func (p *PMUData) UnixNano() int64 {
	seconds := int64(p.SOC)
	fracsec := p.Fracsec & FracsecTimeBaseMask
	nanos := int64(fracsec) * 1e9 / 0x00FFFFFF
	return seconds*1e9 + nanos
}

func (p *PMUData) Magnitude(idx int) float64 {
	if idx < 0 || idx >= len(p.Phasors) {
		return 0
	}
	ph := p.Phasors[idx]
	return math.Sqrt(ph.Real*ph.Real + ph.Imag*ph.Imag)
}

func (p *PMUData) Angle(idx int) float64 {
	if idx < 0 || idx >= len(p.Phasors) {
		return 0
	}
	ph := p.Phasors[idx]
	return math.Atan2(ph.Imag, ph.Real)
}
