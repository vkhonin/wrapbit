package attempter

import (
	"math"
	"time"
)

type ExponentialAttempter struct {
	currAttempt, MaxAttempts uint64
	BaseBackoff              time.Duration
}

func (a *ExponentialAttempter) Attempt() bool {
	if a.currAttempt >= a.MaxAttempts && a.MaxAttempts != 0 {
		return false
	}

	if a.currAttempt > 0 {
		time.Sleep(time.Duration(math.Pow(2, float64(a.currAttempt))) * a.BaseBackoff)
	}

	a.currAttempt++

	return true
}
