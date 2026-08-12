package watchdog

import (
	"fmt"
	"time"

	"github.com/metrico/qryn/v5/reader/model"
	"github.com/metrico/qryn/v5/reader/utils/logger"
)

const (
	// baseInterval is the healthy poll cadence and the first backoff step.
	baseInterval = time.Second * 5
	// maxInterval caps the exponential backoff between failed pings.
	maxInterval = time.Second * 30
)

var (
	svc                 *model.ServiceData
	retries             = 0
	lastSuccessfulCheck = time.Now()
	timer               *time.Timer
	done                chan struct{}
)

func Init(_svc *model.ServiceData) {
	svc = _svc
	timer = time.NewTimer(baseInterval)
	done = make(chan struct{})
	go func() {
		for {
			select {
			case <-done:
				timer.Stop()
				logger.Info("---- WATCHDOG STOPPED ----")
				return
			case <-timer.C:
				err := svc.Ping()
				if err == nil {
					retries = 0
					lastSuccessfulCheck = time.Now()
					logger.Info("---- WATCHDOG CHECK OK ----")
					timer.Reset(baseInterval)
					continue
				}
				retries++
				backoff := nextBackoff(retries)
				logger.Info("---- WATCHDOG REPORT ----")
				logger.Error("database not responding, retry ", retries,
					", next check in ", backoff)
				timer.Reset(backoff)
			}
		}
	}()
}

func nextBackoff(retries int) time.Duration {
	if retries > 3 {
		return maxInterval
	}
	return min(baseInterval<<(retries-1), maxInterval)
}

func Stop() {
	if done != nil {
		done <- struct{}{}
	}
}

func Check() error {
	if lastSuccessfulCheck.Add(time.Second * 30).After(time.Now()) {
		return nil
	}
	return fmt.Errorf("database not responding since %v", lastSuccessfulCheck)
}
