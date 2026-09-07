// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ecsobserver

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	ecstypes "github.com/aws/aws-sdk-go-v2/service/ecs/types"
	"github.com/fsnotify/fsnotify"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"

	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/observer/ecsobserver/internal/ecsmock"
)

func TestNewDiscovery(t *testing.T) {
	logger := zap.NewExample()
	outputFile := "testdata/ut_targets.actual.yaml"
	cfg := Config{
		ClusterName:     "ut-cluster-1",
		ClusterRegion:   "us-test-2",
		RefreshInterval: 10 * time.Millisecond,
		ResultFile:      outputFile,
		JobLabelName:    defaultJobLabelName,
		DockerLabels: []DockerLabelConfig{
			{
				PortLabel:        "PROMETHEUS_PORT",
				JobNameLabel:     "MY_JOB_NAME",
				MetricsPathLabel: "MY_METRICS_PATH",
			},
		},
		Services: []ServiceConfig{
			{
				NamePattern: "s1",
				CommonExporterConfig: CommonExporterConfig{
					MetricsPorts: []int{2112},
				},
			},
		},
	}
	svcNameFilter, err := serviceConfigsToFilter(cfg.Services)
	assert.True(t, svcNameFilter("s1"))
	require.NoError(t, err)
	c := ecsmock.NewClusterWithName(cfg.ClusterName)
	fetcher := newTestTaskFetcher(t, c, c, func(options *taskFetcherOptions) {
		options.Cluster = cfg.ClusterName
		options.serviceNameFilter = svcNameFilter
	})
	opts := serviceDiscoveryOptions{Logger: logger, Fetcher: fetcher}

	// Create 1 task def, 2 services and 11 tasks, 8 running on ec2, first 3 runs on fargate
	nTasks := 11
	nInstances := 2
	nFargateInstances := 3
	c.SetTaskDefinitions(ecsmock.GenTaskDefinitions("d", 2, 1, func(i int, def *ecstypes.TaskDefinition) {
		if i == 0 {
			def.NetworkMode = ecstypes.NetworkModeAwsvpc
		} else {
			def.NetworkMode = ecstypes.NetworkModeBridge
		}
		def.ContainerDefinitions = []ecstypes.ContainerDefinition{
			{
				Name: aws.String("c1"),
				DockerLabels: map[string]string{
					"PROMETHEUS_PORT": "2112",
					"MY_JOB_NAME":     "PROM_JOB_1",
					"MY_METRICS_PATH": "/new/metrics",
				},
				PortMappings: []ecstypes.PortMapping{
					{
						ContainerPort: aws.Int32(2112),
						HostPort:      aws.Int32(2113), // doesn't matter for matcher test
					},
				},
			},
		}
	}))
	c.SetTasks(ecsmock.GenTasks("t", nTasks, func(i int, task *ecstypes.Task) {
		if i < nFargateInstances {
			task.TaskDefinitionArn = aws.String("d0:1")
			task.LaunchType = ecstypes.LaunchTypeFargate
			task.StartedBy = aws.String("deploy0")
			task.Attachments = []ecstypes.Attachment{
				{
					Type: aws.String("ElasticNetworkInterface"),
					Details: []ecstypes.KeyValuePair{
						{
							Name:  aws.String("privateIPv4Address"),
							Value: aws.String(fmt.Sprintf("172.168.1.%d", i)),
						},
					},
				},
			}
			// Pretend this fargate task does not have private ip to trigger print non critical error.
			if i == (nFargateInstances - 1) {
				task.Attachments = nil
			}
		} else {
			ins := i % nInstances
			task.TaskDefinitionArn = aws.String("d1:1")
			task.LaunchType = ecstypes.LaunchTypeEc2
			task.ContainerInstanceArn = aws.String(fmt.Sprintf("ci%d", ins))
			task.StartedBy = aws.String("deploy1")
			task.Containers = []ecstypes.Container{
				{
					Name: aws.String("c1"),
					NetworkBindings: []ecstypes.NetworkBinding{
						{
							ContainerPort: aws.Int32(2112),
							HostPort:      aws.Int32(2114 + int32(i)),
						},
					},
				},
			}
		}
	}))
	// Setting container instance and ec2 is same as previous sub test
	c.SetContainerInstances(ecsmock.GenContainerInstances("ci", nInstances, func(i int, ci *ecstypes.ContainerInstance) {
		ci.Ec2InstanceId = aws.String(fmt.Sprintf("i-%d", i))
	}))
	c.SetEc2Instances(ecsmock.GenEc2Instances("i-", nInstances, func(i int, ins *ec2types.Instance) {
		ins.PublicIpAddress = aws.String(fmt.Sprintf("192.168.1.%d", i))
		ins.PrivateIpAddress = aws.String(fmt.Sprintf("172.168.2.%d", i))
		ins.SubnetId = aws.String(fmt.Sprintf("subnet-%d", i))
		ins.VpcId = aws.String(fmt.Sprintf("vpc-%d", i))
		ins.Tags = []ec2types.Tag{
			{
				Key:   aws.String("aws:cloudformation:instance"),
				Value: aws.String(fmt.Sprintf("cfni%d", i)),
			},
		}
	}))
	// Service
	c.SetServices(ecsmock.GenServices("s", 2, func(i int, s *ecstypes.Service) {
		if i == 0 {
			s.LaunchType = ecstypes.LaunchTypeEc2
			s.Deployments = []ecstypes.Deployment{
				{
					Status: aws.String("ACTIVE"),
					Id:     aws.String("deploy0"),
				},
			}
		} else {
			s.LaunchType = ecstypes.LaunchTypeFargate
			s.Deployments = []ecstypes.Deployment{
				{
					Status: aws.String("ACTIVE"),
					Id:     aws.String("deploy1"),
				},
			}
		}
	}))

	t.Run("success", func(t *testing.T) {
		sd, err := newDiscovery(cfg, opts)
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(t.Context(), cfg.RefreshInterval*2)
		defer cancel()
		err = sd.runAndWriteFile(ctx)
		require.NoError(t, err)

		assert.FileExists(t, outputFile)
		expectedFile := "testdata/ut_targets.expected.yaml"
		// workaround for windows git checkout autocrlf
		// https://circleci.com/blog/circleci-config-teardown-how-we-write-our-circleci-config-at-circleci/#main:~:text=Line%20endings
		expectedContent := bytes.ReplaceAll(mustReadFile(t, expectedFile), []byte("\r\n"), []byte("\n"))
		assert.Equal(t, string(expectedContent), string(mustReadFile(t, outputFile)))
	})

	t.Run("fail to write file", func(t *testing.T) {
		cfg2 := cfg
		cfg2.ResultFile = "testdata/folder/does/not/exists/ut_targets.yaml"
		sd, err := newDiscovery(cfg2, opts)
		require.NoError(t, err)
		require.Error(t, sd.runAndWriteFile(t.Context()))
	})

	t.Run("critical error in discovery", func(t *testing.T) {
		cfg2 := cfg
		cfg2.ClusterName += "not_right_anymore"
		fetcher2 := newTestTaskFetcher(t, c, c, func(options *taskFetcherOptions) {
			options.Cluster = cfg2.ClusterName
			options.serviceNameFilter = svcNameFilter
		})
		opts2 := serviceDiscoveryOptions{Logger: logger, Fetcher: fetcher2}
		sd, err := newDiscovery(cfg2, opts2)
		require.NoError(t, err)
		require.Error(t, sd.runAndWriteFile(t.Context()))
	})

	t.Run("invalid fetcher config", func(t *testing.T) {
		cfg2 := cfg
		cfg2.ClusterName = ""
		opts2 := serviceDiscoveryOptions{Logger: logger}
		_, err := newDiscovery(cfg2, opts2)
		require.Error(t, err)
	})
}

// Util Start

func newTestTaskFilter(t *testing.T, cfg Config) *taskFilter {
	logger := zap.NewExample()
	m, err := newMatchers(cfg, matcherOptions{Logger: logger})
	require.NoError(t, err)
	f := newTaskFilter(logger, m)
	return f
}

func newTestTaskFetcher(t *testing.T, ecsClient ecsClient, ec2Client ec2Client, opts ...func(options *taskFetcherOptions)) *taskFetcher {
	opt := taskFetcherOptions{
		Logger:      zap.NewExample(),
		Cluster:     "not used",
		Region:      "not used",
		ecsOverride: ecsClient,
		ec2Override: ec2Client,
		serviceNameFilter: func(string) bool {
			return true
		},
	}
	for _, m := range opts {
		m(&opt)
	}
	f, err := newTaskFetcher(t.Context(), opt)
	require.NoError(t, err)
	return f
}

func newMatcher(t *testing.T, cfg matcherConfig) targetMatcher {
	m, err := cfg.newMatcher(testMatcherOptions())
	require.NoError(t, err)
	return m
}

func newMatcherAndMatch(t *testing.T, cfg matcherConfig, tasks []*taskAnnotated) *matchResult {
	m := newMatcher(t, cfg)
	res, err := matchContainers(tasks, m, 0)
	require.NoError(t, err)
	return res
}

func testMatcherOptions() matcherOptions {
	return matcherOptions{
		Logger: zap.NewExample(),
	}
}

func mustReadFile(t *testing.T, p string) []byte {
	b, err := os.ReadFile(p)
	require.NoError(t, err, p)
	return b
}

// Util End

// newFileSDPayload returns a marshalled file_sd YAML payload with nTargets entries.
// 1000 targets yields ~432 KiB (~108 pages), large enough that a reader woken by the
// fsnotify event on the first write will arrive before all pages have been written.
func newFileSDPayload(t *testing.T, nTargets int) []byte {
	t.Helper()
	targets := make([]fileSDTarget, nTargets)
	for i := range targets {
		targets[i] = fileSDTarget{
			Targets: []string{fmt.Sprintf("10.0.%d.%d:9090", i/256, i%256)},
			Labels: map[string]string{
				"__meta_ecs_cluster_name":             "prod-cluster",
				"__meta_ecs_container_name":           fmt.Sprintf("container-%d", i),
				"__meta_ecs_task_definition_family":   fmt.Sprintf("task-family-%d", i),
				"__meta_ecs_task_definition_revision": "42",
				"__meta_ecs_task_group":               "service:my-service",
				"__meta_ecs_task_launch_type":         "EC2",
				"__meta_ecs_ec2_instance_id":          fmt.Sprintf("i-0abc%08d", i),
				"__meta_ecs_ec2_instance_type":        "t3.medium",
				"__meta_ecs_ec2_private_ip":           fmt.Sprintf("10.0.%d.%d", i/256, i%256),
			},
		}
	}
	b, err := yaml.Marshal(targets)
	require.NoError(t, err)
	return b
}

// watchForWrite sets up an fsnotify watcher on dir and returns a channel that receives
// one event when any write or create event is observed for the given filename.
func watchForWrite(t *testing.T, dir, filename string) <-chan struct{} {
	t.Helper()
	watcher, err := fsnotify.NewWatcher()
	require.NoError(t, err)
	require.NoError(t, watcher.Add(dir))
	t.Cleanup(func() { _ = watcher.Close() })

	ch := make(chan struct{}, 1)
	go func() {
		for {
			select {
			case event, ok := <-watcher.Events:
				if !ok {
					return
				}
				if filepath.Base(event.Name) == filename &&
					(event.Has(fsnotify.Write) || event.Has(fsnotify.Create)) {
					select {
					case ch <- struct{}{}:
					default:
					}
				}
			case _, ok := <-watcher.Errors:
				if !ok {
					return
				}
			}
		}
	}()
	return ch
}

// TestResultFileWritePartialRead proves that os.WriteFile produces a partial read when
// a reader is woken by the fsnotify event fired during the write. The file is large
// enough (~432 KiB, 108 pages) that the reader arrives before all pages are written.
// This test is specific to Linux inotify semantics and skips on other platforms.
func TestResultFileWritePartialRead(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("partial read race requires Linux inotify semantics")
	}

	const nTargets = 1000
	dir := t.TempDir()
	path := filepath.Join(dir, "targets.yml")
	payload := newFileSDPayload(t, nTargets)

	// Write an initial file so the watcher has something to watch.
	require.NoError(t, os.WriteFile(path, payload, 0o600))

	eventCh := watchForWrite(t, dir, filepath.Base(path))

	// Write the large payload. The fsnotify event fires on the first write syscall,
	// before all pages have been flushed. The reader opens the file at that point.
	go func() {
		require.NoError(t, os.WriteFile(path, payload, 0o600))
	}()

	// Wait for the fsnotify event, then read exactly as prometheus file_sd does.
	select {
	case <-eventCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for fsnotify event")
	}

	b, err := io.ReadAll(func() io.Reader {
		f, err := os.Open(path)
		require.NoError(t, err)
		t.Cleanup(func() { _ = f.Close() })
		return f
	}())
	require.NoError(t, err)

	var result []fileSDTarget
	parseErr := yaml.Unmarshal(b, &result)
	// Either the read produced corrupt YAML (parse error) or fewer targets (silent truncation).
	// Both prove the reader received partial data from the in-progress write.
	if parseErr == nil {
		assert.Less(t, len(result), nTargets,
			"reader woken by fsnotify event got the full payload — expected partial read on Linux")
	}
}

// TestResultFileWriteAtomic proves that writeResultFile eliminates partial reads.
// The fsnotify event fires only after rename(2) completes, at which point the full
// file is already in place. The reader always gets the complete payload.
func TestResultFileWriteAtomic(t *testing.T) {
	const nTargets = 1000
	dir := t.TempDir()
	path := filepath.Join(dir, "targets.yml")
	payload := newFileSDPayload(t, nTargets)

	require.NoError(t, writeResultFile(path, payload))

	eventCh := watchForWrite(t, dir, filepath.Base(path))

	go func() {
		require.NoError(t, writeResultFile(path, payload))
	}()

	select {
	case <-eventCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for fsnotify event")
	}

	b, err := io.ReadAll(func() io.Reader {
		f, err := os.Open(path)
		require.NoError(t, err)
		t.Cleanup(func() { _ = f.Close() })
		return f
	}())
	require.NoError(t, err)

	var result []fileSDTarget
	require.NoError(t, yaml.Unmarshal(b, &result))
	assert.Equal(t, nTargets, len(result))
}
