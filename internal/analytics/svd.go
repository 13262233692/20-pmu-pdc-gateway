package analytics

import (
	"math"
	"math/cmplx"
)

type SVDResult struct {
	U      *ComplexMatrix
	Sigma  []float64
	V      *ComplexMatrix
	Rank   int
}

func SVD(A *ComplexMatrix) *SVDResult {
	return svdJacobi(A)
}

func svdJacobi(A *ComplexMatrix) *SVDResult {
	m := A.Rows
	n := A.Cols

	U := A.Clone()
	V := ComplexEye(n)
	S := make([]float64, n)

	maxSweeps := 50
	tol := 1e-12

	for sweep := 0; sweep < maxSweeps; sweep++ {
		converged := true

		for p := 0; p < n-1; p++ {
			for q := p + 1; q < n; q++ {
				var app, aqq, apq float64

				for i := 0; i < m; i++ {
					up := U.At(i, p)
					uq := U.At(i, q)
					app += real(up)*real(up) + imag(up)*imag(up)
					aqq += real(uq)*real(uq) + imag(uq)*imag(uq)
					apq += real(up)*real(uq) + imag(up)*imag(uq)
				}

				if math.Abs(apq) < tol*math.Sqrt(app*aqq) {
					continue
				}
				converged = false

				theta := 0.5 * math.Atan2(2*apq, app-aqq)
				c := math.Cos(theta)
				s := math.Sin(theta)

				for i := 0; i < m; i++ {
					up := U.At(i, p)
					uq := U.At(i, q)
					U.Set(i, p, complex(c*real(up)-s*real(uq), c*imag(up)-s*imag(uq)))
					U.Set(i, q, complex(s*real(up)+c*real(uq), s*imag(up)+c*imag(uq)))
				}

				for i := 0; i < n; i++ {
					vp := V.At(i, p)
					vq := V.At(i, q)
					V.Set(i, p, complex(c*real(vp)-s*real(vq), c*imag(vp)-s*imag(vq)))
					V.Set(i, q, complex(s*real(vp)+c*real(vq), s*imag(vp)+c*imag(vq)))
				}
			}
		}

		if converged {
			break
		}
	}

	for j := 0; j < n; j++ {
		norm := 0.0
		for i := 0; i < m; i++ {
			v := U.At(i, j)
			norm += real(v)*real(v) + imag(v)*imag(v)
		}
		S[j] = math.Sqrt(norm)
		if S[j] > 1e-15 {
			invS := 1.0 / S[j]
			for i := 0; i < m; i++ {
				v := U.At(i, j)
				U.Set(i, j, complex(real(v)*invS, imag(v)*invS))
			}
		}
	}

	for i := 0; i < n-1; i++ {
		for j := i + 1; j < n; j++ {
			if S[i] < S[j] {
				S[i], S[j] = S[j], S[i]
				for k := 0; k < m; k++ {
					tmp := U.At(k, i)
					U.Set(k, i, U.At(k, j))
					U.Set(k, j, tmp)
				}
				for k := 0; k < n; k++ {
					tmp := V.At(k, i)
					V.Set(k, i, V.At(k, j))
					V.Set(k, j, tmp)
				}
			}
		}
	}

	rank := 0
	for _, s := range S {
		if s > 1e-10*S[0] {
			rank++
		}
	}

	return &SVDResult{
		U:     U,
		Sigma: S,
		V:     V,
		Rank:  rank,
	}
}

func TruncateSVD(svd *SVDResult, rank int) (*ComplexMatrix, *ComplexMatrix, []float64) {
	if rank > len(svd.Sigma) {
		rank = len(svd.Sigma)
	}

	m := svd.U.Rows
	n := svd.V.Rows

	Ur := NewComplexMatrix(m, rank)
	for i := 0; i < m; i++ {
		for j := 0; j < rank; j++ {
			Ur.Set(i, j, svd.U.At(i, j))
		}
	}

	Vr := NewComplexMatrix(n, rank)
	for i := 0; i < n; i++ {
		for j := 0; j < rank; j++ {
			Vr.Set(i, j, svd.V.At(i, j))
		}
	}

	Sr := make([]float64, rank)
	copy(Sr, svd.Sigma[:rank])

	return Ur, Vr, Sr
}

func PinvFromSVD(svd *SVDResult, rank int) *ComplexMatrix {
	if rank > len(svd.Sigma) {
		rank = len(svd.Sigma)
	}

	m := svd.U.Rows
	n := svd.V.Rows

	Ut := svd.U.ConjTranspose()
	Vr := NewComplexMatrix(n, rank)
	for i := 0; i < n; i++ {
		for j := 0; j < rank; j++ {
			Vr.Set(i, j, svd.V.At(i, j))
		}
	}

	UrT := NewComplexMatrix(rank, m)
	for i := 0; i < rank; i++ {
		for j := 0; j < m; j++ {
			UrT.Set(i, j, Ut.At(i, j))
		}
	}

	invS := NewComplexMatrix(rank, rank)
	for i := 0; i < rank; i++ {
		if svd.Sigma[i] > 1e-15 {
			invS.Set(i, i, complex(1.0/svd.Sigma[i], 0))
		}
	}

	tmp := MatMul(invS, UrT)
	return MatMul(Vr, tmp)
}

func GeneralizedEigenvalues(A, B *ComplexMatrix) []complex128 {
	n := A.Rows

	detTol := 1e-10
	result := make([]complex128, 0, n)

	Z := ComplexEye(n)

	for iter := 0; iter < 100; iter++ {
		converged := true

		for p := 0; p < n-1; p++ {
			for q := p + 1; q < n; q++ {
				apq := A.At(p, q)
				aqp := A.At(q, p)
				bpp := B.At(p, p)
				bqq := B.At(q, q)
				bpq := B.At(p, q)
				bqp := B.At(q, p)

				if cmplx.Abs(apq) < detTol && cmplx.Abs(aqp) < detTol &&
					cmplx.Abs(bpq) < detTol && cmplx.Abs(bqp) < detTol {
					continue
				}
				converged = false

				a := bpq
				b := bqq - bpp
				c := -bqp

				disc := b*b - 4*a*c
				sqrtDisc := cmplx.Sqrt(disc)

				t1 := (-b + sqrtDisc) / (2 * a)
				t2 := (-b - sqrtDisc) / (2 * a)

				var t complex128
				if cmplx.Abs(t1) <= cmplx.Abs(t2) {
					t = t1
				} else {
					t = t2
				}

				if cmplx.Abs(a) < 1e-15 {
					if cmplx.Abs(b) > 1e-15 {
						t = -c / b
					} else {
						t = 0
					}
				}

				cs := 1.0 / math.Sqrt(1.0+real(t)*real(t)+imag(t)*imag(t))
				ss := complex(real(t)*cs, imag(t)*cs)
				cc := complex(cs, 0)

				for j := 0; j < n; j++ {
					apj := A.At(p, j)
					aqj := A.At(q, j)
					A.Set(p, j, cc*apj+cmplx.Conj(ss)*aqj)
					A.Set(q, j, -ss*apj+cc*aqj)

					bpj := B.At(p, j)
					bqj := B.At(q, j)
					B.Set(p, j, cc*bpj+cmplx.Conj(ss)*bqj)
					B.Set(q, j, -ss*bpj+cc*bqj)
				}

				for i := 0; i < n; i++ {
					aip := A.At(i, p)
					aiq := A.At(i, q)
					A.Set(i, p, cc*aip+ss*aiq)
					A.Set(i, q, -cmplx.Conj(ss)*aip+cc*aiq)

					bip := B.At(i, p)
					biq := B.At(i, q)
					B.Set(i, p, cc*bip+ss*biq)
					B.Set(i, q, -cmplx.Conj(ss)*bip+cc*biq)
				}

				for i := 0; i < n; i++ {
					zpi := Z.At(i, p)
					zqi := Z.At(i, q)
					Z.Set(i, p, cc*zpi+ss*zqi)
					Z.Set(i, q, -cmplx.Conj(ss)*zpi+cc*zqi)
				}
			}
		}

		if converged {
			break
		}
	}

	for i := 0; i < n; i++ {
		bii := B.At(i, i)
		if cmplx.Abs(bii) > 1e-12 {
			result = append(result, A.At(i, i)/bii)
		}
	}

	return result
}

func QRDecomposition(A *ComplexMatrix) (*ComplexMatrix, *ComplexMatrix) {
	m := A.Rows
	n := A.Cols

	Q := ComplexEye(m)
	R := A.Clone()

	for k := 0; k < n && k < m-1; k++ {
		norm := 0.0
		for i := k; i < m; i++ {
			v := R.At(i, k)
			norm += real(v)*real(v) + imag(v)*imag(v)
		}
		norm = math.Sqrt(norm)

		if norm < 1e-15 {
			continue
		}

		v := make([]complex128, m-k)
		v[0] = R.At(k, k) - complex(norm, 0)
		for i := 1; i < m-k; i++ {
			v[i] = R.At(k+i, k)
		}

		vnorm := 0.0
		for _, val := range v {
			vnorm += real(val)*real(val) + imag(val)*imag(val)
		}
		vnorm = math.Sqrt(vnorm)
		if vnorm < 1e-15 {
			continue
		}
		for i := range v {
			v[i] /= complex(vnorm, 0)
		}

		for j := k; j < n; j++ {
			dot := complex(0, 0)
			for i := 0; i < m-k; i++ {
				dot += cmplx.Conj(v[i]) * R.At(k+i, j)
			}
			for i := 0; i < m-k; i++ {
				R.Set(k+i, j, R.At(k+i, j)-2.0*v[i]*dot)
			}
		}

		for j := 0; j < m; j++ {
			dot := complex(0, 0)
			for i := 0; i < m-k; i++ {
				dot += cmplx.Conj(v[i]) * Q.At(k+i, j)
			}
			for i := 0; i < m-k; i++ {
				Q.Set(k+i, j, Q.At(k+i, j)-2.0*v[i]*dot)
			}
		}
	}

	return Q.ConjTranspose(), R
}
