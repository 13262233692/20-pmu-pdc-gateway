package analytics

import (
	"math"
	"math/cmplx"
)

type ComplexMatrix struct {
	Rows int
	Cols int
	Data []complex128
}

func NewComplexMatrix(rows, cols int) *ComplexMatrix {
	return &ComplexMatrix{
		Rows: rows,
		Cols: cols,
		Data: make([]complex128, rows*cols),
	}
}

func (m *ComplexMatrix) At(i, j int) complex128 {
	return m.Data[i*m.Cols+j]
}

func (m *ComplexMatrix) Set(i, j int, v complex128) {
	m.Data[i*m.Cols+j] = v
}

func (m *ComplexMatrix) Row(i int) []complex128 {
	start := i * m.Cols
	return m.Data[start : start+m.Cols]
}

func (m *ComplexMatrix) Clone() *ComplexMatrix {
	out := NewComplexMatrix(m.Rows, m.Cols)
	copy(out.Data, m.Data)
	return out
}

func MatMul(a, b *ComplexMatrix) *ComplexMatrix {
	if a.Cols != b.Rows {
		panic("matrix dimension mismatch")
	}
	out := NewComplexMatrix(a.Rows, b.Cols)
	for i := 0; i < a.Rows; i++ {
		ar := a.Row(i)
		for k := 0; k < a.Cols; k++ {
			ak := ar[k]
			if ak == 0 {
				continue
			}
			br := b.Row(k)
			or := out.Row(i)
			for j := 0; j < b.Cols; j++ {
				or[j] += ak * br[j]
			}
		}
	}
	return out
}

func MatAdd(a, b *ComplexMatrix) *ComplexMatrix {
	if a.Rows != b.Rows || a.Cols != b.Cols {
		panic("matrix dimension mismatch")
	}
	out := NewComplexMatrix(a.Rows, a.Cols)
	for i := range out.Data {
		out.Data[i] = a.Data[i] + b.Data[i]
	}
	return out
}

func MatSub(a, b *ComplexMatrix) *ComplexMatrix {
	if a.Rows != b.Rows || a.Cols != b.Cols {
		panic("matrix dimension mismatch")
	}
	out := NewComplexMatrix(a.Rows, a.Cols)
	for i := range out.Data {
		out.Data[i] = a.Data[i] - b.Data[i]
	}
	return out
}

func (m *ComplexMatrix) ConjTranspose() *ComplexMatrix {
	out := NewComplexMatrix(m.Cols, m.Rows)
	for i := 0; i < m.Rows; i++ {
		for j := 0; j < m.Cols; j++ {
			out.Set(j, i, cmplx.Conj(m.At(i, j)))
		}
	}
	return out
}

func (m *ComplexMatrix) Transpose() *ComplexMatrix {
	out := NewComplexMatrix(m.Cols, m.Rows)
	for i := 0; i < m.Rows; i++ {
		for j := 0; j < m.Cols; j++ {
			out.Set(j, i, m.At(i, j))
		}
	}
	return out
}

func (m *ComplexMatrix) Scale(s complex128) *ComplexMatrix {
	out := NewComplexMatrix(m.Rows, m.Cols)
	for i, v := range m.Data {
		out.Data[i] = v * s
	}
	return out
}

func (m *ComplexMatrix) NormF() float64 {
	sum := 0.0
	for _, v := range m.Data {
		re := real(v)
		im := imag(v)
		sum += re*re + im*im
	}
	return math.Sqrt(sum)
}

func ComplexEye(n int) *ComplexMatrix {
	m := NewComplexMatrix(n, n)
	for i := 0; i < n; i++ {
		m.Set(i, i, 1)
	}
	return m
}

func ComplexZeros(rows, cols int) *ComplexMatrix {
	return NewComplexMatrix(rows, cols)
}

func ComplexOnes(rows, cols int) *ComplexMatrix {
	m := NewComplexMatrix(rows, cols)
	for i := range m.Data {
		m.Data[i] = 1
	}
	return m
}

func ComplexDiag(v []complex128) *ComplexMatrix {
	n := len(v)
	m := NewComplexMatrix(n, n)
	for i, val := range v {
		m.Set(i, i, val)
	}
	return m
}

func ComplexDiagReal(v []float64) *ComplexMatrix {
	n := len(v)
	m := NewComplexMatrix(n, n)
	for i, val := range v {
		m.Set(i, i, complex(val, 0))
	}
	return m
}

func BuildHankelReal(samples []float64, L int) *ComplexMatrix {
	N := len(samples)
	M := N - L + 1
	H := NewComplexMatrix(L, M)
	for i := 0; i < L; i++ {
		for j := 0; j < M; j++ {
			H.Set(i, j, complex(samples[i+j], 0))
		}
	}
	return H
}

func BuildHankelComplex(samples []complex128, L int) *ComplexMatrix {
	N := len(samples)
	M := N - L + 1
	H := NewComplexMatrix(L, M)
	for i := 0; i < L; i++ {
		for j := 0; j < M; j++ {
			H.Set(i, j, samples[i+j])
		}
	}
	return H
}
