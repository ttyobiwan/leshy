package messages

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"
)

var (
	defaultCleanerInterval = 1 * time.Minute
	defaultCleanerTimeout  = 1 * time.Minute
)

type Cleaner struct{ connMap *ConnectionMap }

func NewCleaner(connMap *ConnectionMap) *Cleaner {
	return &Cleaner{connMap}
}

func (c *Cleaner) Start(ctx context.Context) error {
	ticker := time.NewTicker(defaultCleanerInterval)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			err := c.Clean(ctx)
			if err != nil {
				slog.Error("Error encountered during cleaning", "error", err)
			}
		}
	}
}

func (c *Cleaner) Clean(ctx context.Context) error {
	slog.Info("Starting to clean")

	ctx, cancel := context.WithTimeout(ctx, defaultCleanerTimeout)
	defer cancel()

	wg := sync.WaitGroup{}
	errs := make(chan (error))
	done := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		err := c.removeStaleConnections()
		if err != nil {
			errs <- fmt.Errorf("removing stale connections: %w", err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		err := c.removeOldMessages()
		if err != nil {
			errs <- fmt.Errorf("removing old messages: %w", err)
		}
	}()

	go func() {
		wg.Wait()
		done <- struct{}{}
	}()

	errMsgs := []string{}

cleaner:
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-done:
			break cleaner
		case err := <-errs:
			errMsgs = append(errMsgs, err.Error())
		}
	}

	slog.Info("Done cleaning")

	var err error
	if len(errMsgs) != 0 {
		err = errors.New(strings.Join(errMsgs, "; "))
	}
	if err != nil {
		return fmt.Errorf("cleaning: %w", err)
	}

	return err
}

func (c *Cleaner) removeStaleConnections() error {
	removedCount := c.connMap.Clean()
	slog.Info("Done cleaning stale connections", "removed", removedCount)
	return nil
}

func (c *Cleaner) removeOldMessages() error {
	return nil
}
