package attempter

import "time"

type FibonacciAttempter struct {
	currAttempt, MaxAttempts              uint64
	InitBackoff, prevBackoff, currBackoff time.Duration
}

func (a *FibonacciAttempter) Attempt() bool {
	if a.currAttempt >= a.MaxAttempts && a.MaxAttempts != 0 {
		return false
	}

	if a.currAttempt > 0 {
		time.Sleep(time.Duration(a.currAttempt) * a.currBackoff)
		a.prevBackoff, a.currBackoff = a.currBackoff, a.prevBackoff+a.currBackoff
	} else {
		a.currBackoff = a.InitBackoff
	}

	a.currAttempt++

	return true
}
