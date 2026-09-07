// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ecsobserver // import "github.com/open-telemetry/opentelemetry-collector-contrib/extension/observer/ecsobserver"

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"go.uber.org/zap"
)

// serviceDiscovery runs the discovery loop.
// It writes discovered targets as prometheus file sd format (for now).
type serviceDiscovery struct {
	logger   *zap.Logger
	cfg      Config
	fetcher  *taskFetcher
	filter   *taskFilter
	exporter *taskExporter
}

type serviceDiscoveryOptions struct {
	Logger  *zap.Logger
	Fetcher *taskFetcher // mock server in test, otherwise call new newTaskFetcherFromConfig to use AWS API
}

func newDiscovery(cfg Config, opts serviceDiscoveryOptions) (*serviceDiscovery, error) {
	if opts.Fetcher == nil {
		return nil, errors.New("fetcher is nil")
	}
	matchers, err := newMatchers(cfg, matcherOptions{Logger: opts.Logger})
	if err != nil {
		return nil, fmt.Errorf("init matchers failed: %w", err)
	}
	filter := newTaskFilter(opts.Logger, matchers)
	exporter := newTaskExporter(opts.Logger, cfg.ClusterName)
	return &serviceDiscovery{
		logger:   opts.Logger,
		cfg:      cfg,
		fetcher:  opts.Fetcher,
		filter:   filter,
		exporter: exporter,
	}, nil
}

// runAndWriteFile writes the output to Config.ResultFile.
func (s *serviceDiscovery) runAndWriteFile(ctx context.Context) error {
	ticker := time.NewTicker(s.cfg.RefreshInterval)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			targets, err := s.discover(ctx)
			s.logger.Debug("Discovered targets", zap.Int("Count", len(targets)))
			if err != nil {
				// Stop on critical error
				if cerr := hasCriticalError(s.logger, err); cerr != nil {
					return cerr
				}
				// Print all the minor errors for debugging, e.g. user config etc.
				printErrors(s.logger, err)
			}
			// We may get 0 targets form some recoverable errors
			// e.g. throttled, in that case we keep existing exported file.
			if len(targets) == 0 && err != nil {
				// We already printed th error
				s.logger.Warn("Skip generating empty target file because of previous errors")
				continue
			}

			// As long as we have some targets, export them regardless of errors.
			// A better approach might be keep previous targets in memory and do a diff and merge on error.
			// For now we just replace entire exported file.

			// Encoding and file write error should never happen,
			// so we stop extension by returning error.
			b, err := targetsToFileSDYAML(targets, s.cfg.JobLabelName)
			if err != nil {
				return err
			}
			// NOTE: We assume the folder already exists and does NOT try to create one.
			if err := writeResultFile(s.cfg.ResultFile, b); err != nil {
				return err
			}
		}
	}
}

// writeResultFile atomically writes b to path via a temp file and rename.
// os.CreateTemp creates the file with mode 0o600 (matching the legacy os.WriteFile
// call), and os.Rename preserves that mode when moving it into place.
func writeResultFile(path string, b []byte) error {
	f, err := os.CreateTemp(filepath.Dir(path), ".ecs_sd-*.tmp")
	if err != nil {
		return err
	}
	tmpName := f.Name()
	defer func() { _ = os.Remove(tmpName) }() // no-op once renamed successfully
	if _, err := f.Write(b); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, path)
}

// discover fetch tasks, filter by matching result and export them.
func (s *serviceDiscovery) discover(ctx context.Context) ([]prometheusECSTarget, error) {
	tasks, err := s.fetcher.fetchAndDecorate(ctx)
	if err != nil {
		return nil, err
	}
	filtered, err := s.filter.filter(tasks)
	if err != nil {
		return nil, err
	}
	return s.exporter.exportTasks(filtered)
}
