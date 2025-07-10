package attempter

import "time"

type ConstantAttempter struct {
	CurrAttempt uint64
	InitBackoff time.Duration
	MaxAttempts uint64
}

func (a *ConstantAttempter) Attempt() bool {
	if a.CurrAttempt >= a.MaxAttempts && a.MaxAttempts != 0 {
		return false
	}

	if a.CurrAttempt > 0 {
		time.Sleep(a.InitBackoff)
	}

	a.CurrAttempt++

	return true
}
