package analytics

import (
	"math"
	"math/cmplx"
	"sort"
)

type OscillationMode struct {
	Frequency     float64
	DampingRatio  float64
	DampingFactor float64
	Amplitude     float64
	Phase         float64
	Pole          complex128
	Energy        float64
}

type MPMResult struct {
	Modes   []OscillationMode
	Order   int
	Residue float64
}

type MPMConfig struct {
	WindowSize     int
	PencilParam    int
	MaxModes       int
	SampleRate     float64
	MinFrequency   float64
	MaxFrequency   float64
	SVDThreshold   float64
	MinDamping     float64
	MinAmplitude   float64
}

func DefaultMPMConfig(sampleRate float64) MPMConfig {
	windowSize := int(5.0 * sampleRate)
	return MPMConfig{
		WindowSize:   windowSize,
		PencilParam:  windowSize / 2,
		MaxModes:     20,
		SampleRate:   sampleRate,
		MinFrequency: 0.1,
		MaxFrequency: 2.5,
		SVDThreshold: 0.01,
		MinDamping:   -0.5,
		MinAmplitude: 1e-6,
	}
}

func MatrixPencilMethod(samples []float64, cfg MPMConfig) *MPMResult {
	N := len(samples)
	L := cfg.PencilParam
	if L <= 0 || L >= N {
		L = N / 2
	}

	mean := 0.0
	for _, x := range samples {
		mean += x
	}
	mean /= float64(N)

	centered := make([]float64, N)
	for i, x := range samples {
		centered[i] = x - mean
	}

	Y := BuildHankelReal(centered, L)

	svd := SVD(Y)

	P := estimateModelOrder(svd.Sigma, cfg.SVDThreshold, cfg.MaxModes)
	if P <= 0 {
		return &MPMResult{Modes: []OscillationMode{}, Order: 0, Residue: 0}
	}

	U1 := NewComplexMatrix(L-1, P)
	for i := 0; i < L-1; i++ {
		for j := 0; j < P; j++ {
			U1.Set(i, j, svd.U.At(i, j))
		}
	}

	U2 := NewComplexMatrix(L-1, P)
	for i := 0; i < L-1; i++ {
		for j := 0; j < P; j++ {
			U2.Set(i, j, svd.U.At(i+1, j))
		}
	}

	U2H := U2.ConjTranspose()

	A := MatMul(U2H, U1)
	B := MatMul(U2H, U2)

	poles := GeneralizedEigenvalues(A, B)

	dt := 1.0 / cfg.SampleRate
	modes := make([]OscillationMode, 0, len(poles))

	for _, z := range poles {
		if cmplx.Abs(z) < 1e-10 {
			continue
		}

		s := cmplx.Log(z) / complex(dt, 0)

		sigma := real(s)
		omega := imag(s)
		freq := math.Abs(omega) / (2 * math.Pi)

		if freq < cfg.MinFrequency || freq > cfg.MaxFrequency {
			continue
		}

		if sigma < cfg.MinDamping {
			continue
		}

		sMag := math.Sqrt(sigma*sigma + omega*omega)
		dampingRatio := -sigma / sMag
		if sMag < 1e-15 {
			dampingRatio = 0
		}

		amp, phase := estimateAmplitudePhase(centered, z, dt)

		modes = append(modes, OscillationMode{
			Frequency:     freq,
			DampingRatio:  dampingRatio,
			DampingFactor: sigma,
			Amplitude:     amp,
			Phase:         phase,
			Pole:          s,
			Energy:        amp * amp,
		})
	}

	sort.Slice(modes, func(i, j int) bool {
		return modes[i].Energy > modes[j].Energy
	})

	if len(modes) > cfg.MaxModes {
		modes = modes[:cfg.MaxModes]
	}

	residue := computeResidue(centered, modes, dt)

	return &MPMResult{
		Modes:   modes,
		Order:   len(modes),
		Residue: residue,
	}
}

func estimateModelOrder(sigma []float64, threshold float64, maxModes int) int {
	if len(sigma) == 0 || sigma[0] < 1e-15 {
		return 0
	}

	P := 0
	maxSig := sigma[0]
	for i, s := range sigma {
		if s/maxSig > threshold {
			P = i + 1
		} else {
			break
		}
	}

	if P > maxModes {
		P = maxModes
	}

	if P == 0 {
		P = 1
	}

	return P
}

func estimateAmplitudePhase(samples []float64, pole complex128, dt float64) (float64, float64) {
	N := len(samples)
	bestAmp := 0.0
	bestPhase := 0.0
	bestErr := math.MaxFloat64

	for amp := 0.001; amp < 10.0; amp *= 1.5 {
		for phase := -math.Pi; phase < math.Pi; phase += 0.1 {
			err := 0.0
			for n := 0; n < N; n++ {
				t := float64(n) * dt
				modeled := amp * math.Exp(real(pole)*t) * math.Cos(imag(pole)*t+phase)
				diff := samples[n] - modeled
				err += diff * diff
			}
			if err < bestErr {
				bestErr = err
				bestAmp = amp
				bestPhase = phase
			}
		}
	}

	return bestAmp, bestPhase
}

func computeResidue(samples []float64, modes []OscillationMode, dt float64) float64 {
	N := len(samples)
	total := 0.0
	signalPower := 0.0

	for n := 0; n < N; n++ {
		signalPower += samples[n] * samples[n]
		reconstructed := 0.0
		t := float64(n) * dt
		for _, m := range modes {
			s := complex(m.DampingFactor, 2*math.Pi*m.Frequency)
			reconstructed += m.Amplitude * math.Exp(real(s)*t) * math.Cos(imag(s)*t+m.Phase)
		}
		diff := samples[n] - reconstructed
		total += diff * diff
	}

	if signalPower < 1e-15 {
		return 0
	}
	return math.Sqrt(total / signalPower)
}

func DominantMode(result *MPMResult) *OscillationMode {
	if len(result.Modes) == 0 {
		return nil
	}
	best := &result.Modes[0]
	for i := range result.Modes {
		if result.Modes[i].Energy > best.Energy {
			best = &result.Modes[i]
		}
	}
	return best
}

func FindLowestDampingMode(result *MPMResult, minFreq, maxFreq float64) *OscillationMode {
	var best *OscillationMode
	for i := range result.Modes {
		m := &result.Modes[i]
		if m.Frequency < minFreq || m.Frequency > maxFreq {
			continue
		}
		if best == nil || m.DampingRatio < best.DampingRatio {
			best = m
		}
	}
	return best
}

func (m *OscillationMode) IsStable() bool {
	return m.DampingFactor < 0
}

func (m *OscillationMode) IsWarning(threshold float64) bool {
	return m.DampingRatio >= 0 && m.DampingRatio < threshold
}

func (m *OscillationMode) IsCritical(threshold float64) bool {
	return m.DampingRatio >= 0
}
