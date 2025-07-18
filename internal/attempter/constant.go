package attempter

import "time"

type ConstantAttempter struct {
	currAttempt, MaxAttempts uint64
	Backoff                  time.Duration
}

func (a *ConstantAttempter) Attempt() bool {
	if a.currAttempt >= a.MaxAttempts && a.MaxAttempts != 0 {
		return false
	}

	if a.currAttempt > 0 {
		time.Sleep(a.Backoff)
	}

	a.currAttempt++

	return true
}
