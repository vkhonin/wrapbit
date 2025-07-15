package utils

type Retry func() Attempter

type Attempter interface {
	Attempt() bool
}
