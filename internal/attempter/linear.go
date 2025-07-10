package attempter

import "time"

type LinearAttempter struct {
	currAttempt, MaxAttempts uint64
	Backoff                  time.Duration
}

func (a *LinearAttempter) Attempt() bool {
	if a.currAttempt >= a.MaxAttempts && a.MaxAttempts != 0 {
		return false
	}

	if a.currAttempt > 0 {
		time.Sleep(time.Duration(a.currAttempt) * a.Backoff)
	}

	a.currAttempt++

	return true
}
