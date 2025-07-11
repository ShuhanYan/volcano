/*
Copyright 2024 The Volcano Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package local

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/prompb"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"

	"volcano.sh/volcano/pkg/agent/utils/cgroup"
)

const (
	defaultProcPathV2 = "/host/proc/stat"
	procStatPathEnvV2 = "PROC_STAT_PATH"
)

type CPUResourceCollectorV2 struct {
	cgroupManager cgroup.CgroupManager
}

func NewCPUResourceCollectorV2(cgroupManager cgroup.CgroupManager) (SubCollector, error) {
	return &CPUResourceCollectorV2{
		cgroupManager: cgroupManager,
	}, nil
}

func (c *CPUResourceCollectorV2) Run() {}

func (c *CPUResourceCollectorV2) CollectLocalMetrics(metricInfo *LocalMetricInfo, start time.Time, window metav1.Duration) ([]*prompb.TimeSeries, error) {
	cgroupPath, err := c.cgroupManager.GetRootCgroupPath(cgroup.CgroupCpuSubsystem)
	if err != nil {
		return nil, err
	}

	podAllUsage, err := getMilliCPUUsageV2(cgroupPath)
	if err != nil {
		return nil, err
	}

	finalUsage := int64(0)
	if !metricInfo.IncludeGuaranteedPods {
		for _, qos := range []corev1.PodQOSClass{corev1.PodQOSBurstable, corev1.PodQOSBestEffort} {
			cgroupPath, err = c.cgroupManager.GetQoSCgroupPath(qos, cgroup.CgroupCpuSubsystem)
			if err != nil {
				return nil, err
			}

			count, err := getMilliCPUUsageV2(cgroupPath)
			if err != nil {
				return nil, err
			}
			finalUsage += count
		}
	} else {
		finalUsage = podAllUsage
	}

	if metricInfo.IncludeSystemUsed {
		nodeUsage, err := nodeCPUUsageV2()
		if err != nil {
			return nil, fmt.Errorf("failed to get node usage, err: %v", err)
		}
		systemUsage := nodeUsage - podAllUsage
		finalUsage += systemUsage
	}
	sample := prompb.TimeSeries{
		Samples: []prompb.Sample{
			{
				Timestamp: timestamp.FromTime(time.Now()),
				Value:     float64(finalUsage),
			},
		},
	}
	return []*prompb.TimeSeries{&sample}, nil
}

// getMilliCPUUsageV2 reads cgroup v2 cpu.stat usage_usec and returns millicores used per second
func getMilliCPUUsageV2(cgroupRoot string) (int64, error) {
	cpuStatFile := filepath.Join(cgroupRoot, "cpu.stat")
	startUsage, err := readUsageUsec(cpuStatFile)
	if err != nil {
		return 0, err
	}
	time.Sleep(1 * time.Second)
	endUsage, err := readUsageUsec(cpuStatFile)
	if err != nil {
		return 0, err
	}
	if endUsage < startUsage {
		return 0, fmt.Errorf("cpu usage decreased unexpectedly")
	}
	// usage_usec is in microseconds, so (end-start) is microseconds in 1s
	// 1 core = 1_000_000 us/s, so millicores = (delta / 1_000)
	return (endUsage - startUsage) / 1000, nil
}

func readUsageUsec(cpuStatFile string) (int64, error) {
	contents, err := os.ReadFile(cpuStatFile)
	if err != nil {
		return 0, err
	}
	for _, line := range strings.Split(string(contents), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 && fields[0] == "usage_usec" {
			val, err := strconv.ParseInt(fields[1], 10, 64)
			if err != nil {
				return 0, err
			}
			return val, nil
		}
	}
	return 0, fmt.Errorf("usage_usec not found in cpu.stat")
}

func nodeCPUStateV2() (uint64, error) {
	procStatFile := os.Getenv(procStatPathEnvV2)
	if procStatFile == "" {
		procStatFile = defaultProcPathV2
	}
	contents, err := os.ReadFile(procStatFile)
	if err != nil {
		return 0, fmt.Errorf("failed to read proc state file: %v", err)
	}
	for _, line := range strings.Split(string(contents), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 || fields[0] != "cpu" {
			continue
		}
		total := uint64(0)
		for i := 1; i < len(fields); i++ {
			val, err := strconv.ParseUint(fields[i], 10, 64)
			if err != nil {
				return 0, fmt.Errorf("failed to parse state filed: %v, err: %v", fields[i], err)
			}
			if i == 4 || i == 5 {
				continue
			}
			total += val
		}
		return total, nil
	}
	return 0, fmt.Errorf("invalid proc stat file: %s", procStatFile)
}

func nodeCPUUsageV2() (int64, error) {
	startTime := time.Now().UnixNano()
	total0, err := nodeCPUStateV2()
	if err != nil {
		return 0, err
	}
	time.Sleep(time.Second)
	endTime := time.Now().UnixNano()
	total1, err := nodeCPUStateV2()
	if err != nil {
		return 0, err
	}

	totalTicks := float64(total1 - total0)
	if totalTicks <= 0 {
		return 0, fmt.Errorf("negative total tick: %v", totalTicks)
	}
	// jiffies = 10ms, same as v1
	jiffies := float64(10 * time.Millisecond)
	cpuUsage := int64((totalTicks / (float64(endTime-startTime) / jiffies)) * 1000)
	klog.V(4).InfoS("Node cpu usage (v2)", "value", cpuUsage)
	return cpuUsage, nil
}
