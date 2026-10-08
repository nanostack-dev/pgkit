package workflow

import (
	"math"
	"time"
)

// DefineOption configures a workflow: Version or a default Retry for its steps.
type DefineOption interface {
	applyToDefinition(*definition)
}

// StartOption configures a run: Key or Timeout.
type StartOption interface {
	applyToStart(*startConfig)
}

// StepOption configures a step: Retry or Timeout.
type StepOption interface {
	applyToStep(*stepConfig)
}

type startConfig struct {
	key     string
	timeout time.Duration
}

type stepConfig struct {
	retry   *Retry
	timeout time.Duration
}

// Retry is how many times a step is attempted and how long it waits between
// attempts. Pass it to Define to set the policy of every step of a workflow, or to
// a step to override some of it: a step's zero fields keep the workflow's values.
// Fields still zero take the defaults: 3 attempts, the first retry after one second,
// doubling up to one minute (or up to Backoff, when that is longer).
type Retry struct {
	MaxAttempts int
	Backoff     time.Duration
	MaxBackoff  time.Duration
}

// NoRetry attempts a step once.
var NoRetry = Retry{MaxAttempts: 1}

const (
	defaultMaxAttempts = 3
	defaultBackoff     = time.Second
	defaultMaxBackoff  = time.Minute
)

func (r Retry) applyToStep(c *stepConfig) {
	c.retry = &r
}

func (r Retry) applyToDefinition(d *definition) {
	d.retry = r
}

func (r Retry) overriddenBy(override Retry) Retry {
	if override.MaxAttempts > 0 {
		r.MaxAttempts = override.MaxAttempts
	}
	if override.Backoff > 0 {
		r.Backoff = override.Backoff
	}
	if override.MaxBackoff > 0 {
		r.MaxBackoff = override.MaxBackoff
	}
	return r
}

func (r Retry) withDefaults() Retry {
	if r.MaxAttempts <= 0 {
		r.MaxAttempts = defaultMaxAttempts
	}
	if r.Backoff <= 0 {
		r.Backoff = defaultBackoff
	}
	if r.MaxBackoff <= 0 {
		r.MaxBackoff = max(defaultMaxBackoff, r.Backoff)
	}
	return r
}

// delayAfter is the wait before the attempt that follows failed attempt number
// attempt: Backoff, then doubled each time, capped at MaxBackoff.
func (r Retry) delayAfter(attempt int) time.Duration {
	doublings := float64(max(attempt-1, 0))
	delay := float64(r.Backoff) * math.Pow(2, doublings)
	if delay > float64(r.MaxBackoff) {
		return r.MaxBackoff
	}
	return time.Duration(delay)
}

// Timeout bounds one attempt of a step, or a whole run when passed to Start. A run
// that outlives its timeout fails, and errors.Is(err, ErrTimeout) reports it.
type Timeout time.Duration

func (t Timeout) applyToStep(c *stepConfig) {
	c.timeout = time.Duration(t)
}

func (t Timeout) applyToStart(c *startConfig) {
	c.timeout = time.Duration(t)
}

// Key makes Start idempotent: starting the same workflow again with the same key
// returns the existing run, whatever its status, instead of starting another.
type Key string

func (k Key) applyToStart(c *startConfig) {
	c.key = string(k)
}

// Version separates incompatible versions of a workflow. A run is executed only by
// workers that list the version it started with, so keep the old definition
// registered until its runs finish. Versions are 0 when not set.
type Version int

func (v Version) applyToDefinition(d *definition) {
	d.version = int(v)
}

// Forever makes Receive wait for its signal without a timeout.
const Forever time.Duration = math.MaxInt64
