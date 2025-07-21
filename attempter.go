package wrapbit

import (
	"math"
	"time"
)

type Retry func() Attempter

type Attempter interface {
	Attempt() bool
}

type constantAttempter struct {
	currAttempt, maxAttempts uint64
	backoff                  time.Duration
}

func (a *constantAttempter) Attempt() bool {
	if a.currAttempt >= a.maxAttempts && a.maxAttempts != 0 {
		return false
	}

	if a.currAttempt > 0 {
		time.Sleep(a.backoff)
	}

	a.currAttempt++

	return true
}

type exponentialAttempter struct {
	currAttempt, maxAttempts uint64
	baseBackoff              time.Duration
}

func (a *exponentialAttempter) Attempt() bool {
	if a.currAttempt >= a.maxAttempts && a.maxAttempts != 0 {
		return false
	}

	if a.currAttempt > 0 {
		time.Sleep(time.Duration(math.Pow(2, float64(a.currAttempt))) * a.baseBackoff)
	}

	a.currAttempt++

	return true
}

type fibonacciAttempter struct {
	currAttempt, maxAttempts              uint64
	initBackoff, prevBackoff, currBackoff time.Duration
}

func (a *fibonacciAttempter) Attempt() bool {
	if a.currAttempt >= a.maxAttempts && a.maxAttempts != 0 {
		return false
	}

	if a.currAttempt > 0 {
		time.Sleep(time.Duration(a.currAttempt) * a.currBackoff)
		a.prevBackoff, a.currBackoff = a.currBackoff, a.prevBackoff+a.currBackoff
	} else {
		a.currBackoff = a.initBackoff
	}

	a.currAttempt++

	return true
}

type linearAttempter struct {
	currAttempt, maxAttempts uint64
	backoff                  time.Duration
}

func (a *linearAttempter) Attempt() bool {
	if a.currAttempt >= a.maxAttempts && a.maxAttempts != 0 {
		return false
	}

	if a.currAttempt > 0 {
		time.Sleep(time.Duration(a.currAttempt) * a.backoff)
	}

	a.currAttempt++

	return true
}

type polynomialAttempter struct {
	currAttempt, degree, maxAttempts uint64
	baseBackoff                      time.Duration
}

func (a *polynomialAttempter) Attempt() bool {
	if a.currAttempt >= a.maxAttempts && a.maxAttempts != 0 {
		return false
	}

	if a.currAttempt > 0 {
		time.Sleep(time.Duration(math.Pow(float64(a.currAttempt), float64(a.degree))) * a.baseBackoff)
	}

	a.currAttempt++

	return true
}
