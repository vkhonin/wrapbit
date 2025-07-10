package attempter

import (
	"math"
	"time"
)

type PolynomialAttempter struct {
	currAttempt, Degree, MaxAttempts uint64
	BaseBackoff                      time.Duration
}

func (a *PolynomialAttempter) Attempt() bool {
	if a.currAttempt >= a.MaxAttempts && a.MaxAttempts != 0 {
		return false
	}

	if a.currAttempt > 0 {
		time.Sleep(time.Duration(math.Pow(float64(a.currAttempt), float64(a.Degree))) * a.BaseBackoff)
	}

	a.currAttempt++

	return true
}
