package collector

import (
	"bufio"
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

type cpuRawState struct {
	user    uint64
	nice    uint64
	system  uint64
	idle    uint64
	iowait  uint64
	irq     uint64
	softirq uint64
	steal   uint64
}

func (s cpuRawState) total() uint64 {
	return s.user + s.nice + s.system + s.idle + s.iowait + s.irq + s.softirq + s.steal
}

func (s cpuRawState) idleTime() uint64 {
	return s.idle + s.iowait
}

// CPUCollector monitors detailed CPU performance, load, frequencies, temperature, and power.
type CPUCollector struct {
	mu              sync.Mutex
	procStatPath    string
	loadavgPath     string
	powercapPath    string
	prevStates      map[string]cpuRawState
	prevPowerEnergy uint64
	prevPowerTime   time.Time
	hasPrev         bool
}

// NewCPUCollector constructs a CPUCollector with default paths.
func NewCPUCollector() *CPUCollector {
	return &CPUCollector{
		procStatPath: "/proc/stat",
		loadavgPath:  "/proc/loadavg",
		powercapPath: "/sys/class/powercap/intel-rapl:0/energy_uj",
		prevStates:   make(map[string]cpuRawState),
	}
}

// Name returns the name of the collector.
func (c *CPUCollector) Name() string {
	return "cpu"
}

// Collect returns the gathered CPU metric event payload.
func (c *CPUCollector) Collect(ctx context.Context) ([]Event, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	currentStates, err := parseProcStat(c.procStatPath)
	if err != nil {
		return nil, fmt.Errorf("failed to parse proc/stat: %w", err)
	}

	if !c.hasPrev {
		c.prevStates = currentStates
		c.hasPrev = true

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}

		currentStates, err = parseProcStat(c.procStatPath)
		if err != nil {
			return nil, fmt.Errorf("failed to parse proc/stat for delta: %w", err)
		}
	}

	globalCur, ok := currentStates["cpu"]
	if !ok {
		return nil, fmt.Errorf("missing global cpu state in /proc/stat")
	}
	globalPrev, ok := c.prevStates["cpu"]
	if !ok {
		globalPrev = globalCur
	}

	deltaTotal := globalCur.total() - globalPrev.total()
	deltaIdleTime := globalCur.idleTime() - globalPrev.idleTime()
	deltaActive := deltaTotal - deltaIdleTime

	var usagePercent float64
	var userPercent float64
	var sysPercent float64
	var idlePercent float64
	var iowaitPercent float64
	var stealPercent float64

	if deltaTotal > 0 {
		usagePercent = (float64(deltaActive) / float64(deltaTotal)) * 100
		userPercent = (float64((globalCur.user-globalPrev.user)+(globalCur.nice-globalPrev.nice)) / float64(deltaTotal)) * 100
		sysPercent = (float64((globalCur.system-globalPrev.system)+(globalCur.irq-globalPrev.irq)+(globalCur.softirq-globalPrev.softirq)) / float64(deltaTotal)) * 100
		idlePercent = (float64(globalCur.idle-globalPrev.idle) / float64(deltaTotal)) * 100
		iowaitPercent = (float64(globalCur.iowait-globalPrev.iowait) / float64(deltaTotal)) * 100
		stealPercent = (float64(globalCur.steal-globalPrev.steal) / float64(deltaTotal)) * 100
	}

	l1, l5, l15, _ := getLoadAverages(c.loadavgPath)
	freqs := getCPUFrequencies()

	var freqSum float64
	freqMin := math.MaxFloat64
	freqMax := 0.0
	for _, f := range freqs {
		freqSum += f
		if f < freqMin {
			freqMin = f
		}
		if f > freqMax {
			freqMax = f
		}
	}
	var freqAvg float64
	if len(freqs) > 0 {
		freqAvg = freqSum / float64(len(freqs))
	} else {
		freqMin = 0.0
	}

	tempAvg, tempMax, tempOk := getCPUTemperatures()
	power := c.getPowerWatts()

	var perCPUMetrics []any
	for name, cur := range currentStates {
		if name == "cpu" {
			continue
		}
		prev, ok := c.prevStates[name]
		if !ok {
			prev = cur
		}
		dTotal := cur.total() - prev.total()
		dIdleTime := cur.idleTime() - prev.idleTime()
		var cpuUsage float64
		if dTotal > 0 {
			cpuUsage = (float64(dTotal-dIdleTime) / float64(dTotal)) * 100
		}

		cpuInfo := map[string]any{
			"cpu":           name,
			"usage_percent": cpuUsage,
		}
		if freq, found := freqs[name]; found {
			cpuInfo["frequency_mhz"] = freq
		}
		perCPUMetrics = append(perCPUMetrics, cpuInfo)
	}

	c.prevStates = currentStates

	data := map[string]any{
		"usage_percent":    usagePercent,
		"user_percent":     userPercent,
		"system_percent":   sysPercent,
		"idle_percent":     idlePercent,
		"iowait_percent":   iowaitPercent,
		"steal_percent":    stealPercent,
		"load_average_1m":  l1,
		"load_average_5m":  l5,
		"load_average_15m": l15,
		"per_cpu":          perCPUMetrics,
	}

	if len(freqs) > 0 {
		data["frequency_mhz_avg"] = freqAvg
		data["frequency_mhz_min"] = freqMin
		data["frequency_mhz_max"] = freqMax
	}
	if tempOk {
		data["temperature_celsius_avg"] = tempAvg
		data["temperature_celsius_max"] = tempMax
	}
	if power > 0 {
		data["power_watts"] = power
	}

	return []Event{
		{
			Event:     "metric_sample",
			Collector: "cpu",
			Data:      data,
		},
	}, nil
}

func parseProcStat(path string) (map[string]cpuRawState, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	states := make(map[string]cpuRawState)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		if strings.HasPrefix(fields[0], "cpu") {
			name := fields[0]
			if len(fields) < 9 {
				continue
			}

			var vals [8]uint64
			for i := range 8 {
				v, err := strconv.ParseUint(fields[i+1], 10, 64)
				if err != nil {
					return nil, err
				}
				vals[i] = v
			}

			states[name] = cpuRawState{
				user:    vals[0],
				nice:    vals[1],
				system:  vals[2],
				idle:    vals[3],
				iowait:  vals[4],
				irq:     vals[5],
				softirq: vals[6],
				steal:   vals[7],
			}
		}
	}
	return states, scanner.Err()
}

func getCPUFrequencies() map[string]float64 {
	freqs := make(map[string]float64)
	matches, err := filepath.Glob("/sys/devices/system/cpu/cpu[0-9]*/cpufreq/scaling_cur_freq")
	if err == nil && len(matches) > 0 {
		for _, m := range matches {
			parts := strings.Split(m, "/")
			if len(parts) >= 6 {
				cpuName := parts[5]
				if data, err := os.ReadFile(m); err == nil {
					if val, err := strconv.ParseFloat(strings.TrimSpace(string(data)), 64); err == nil {
						freqs[cpuName] = val / 1000.0
					}
				}
			}
		}
	}
	if len(freqs) == 0 {
		if file, err := os.Open("/proc/cpuinfo"); err == nil {
			defer file.Close()
			scanner := bufio.NewScanner(file)
			cpuIdx := 0
			for scanner.Scan() {
				line := scanner.Text()
				if strings.HasPrefix(line, "cpu MHz") {
					parts := strings.SplitN(line, ":", 2)
					if len(parts) == 2 {
						if mhz, err := strconv.ParseFloat(strings.TrimSpace(parts[1]), 64); err == nil {
							freqs[fmt.Sprintf("cpu%d", cpuIdx)] = mhz
							cpuIdx++
						}
					}
				}
			}
			if err := scanner.Err(); err != nil {
				// Handle or ignore scanner error
			}
		}
	}
	return freqs
}

func getCPUTemperatures() (avgVal, maxVal float64, ok bool) {
	var temps []float64
	matches, err := filepath.Glob("/sys/class/hwmon/hwmon*/name")
	if err == nil && len(matches) > 0 {
		for _, m := range matches {
			if nameData, err := os.ReadFile(m); err == nil {
				name := strings.TrimSpace(string(nameData))
				if name == "coretemp" || name == "k10temp" || name == "acpitz" {
					dir := filepath.Dir(m)
					inputs, err := filepath.Glob(filepath.Join(dir, "temp*_input"))
					if err == nil {
						for _, input := range inputs {
							if tempData, err := os.ReadFile(input); err == nil {
								if millideg, err := strconv.ParseFloat(strings.TrimSpace(string(tempData)), 64); err == nil {
									temps = append(temps, millideg/1000.0)
								}
							}
						}
					}
				}
			}
		}
	}
	if len(temps) == 0 {
		matches, err := filepath.Glob("/sys/class/thermal/thermal_zone*/temp")
		if err == nil {
			for _, m := range matches {
				if tempData, err := os.ReadFile(m); err == nil {
					if millideg, err := strconv.ParseFloat(strings.TrimSpace(string(tempData)), 64); err == nil {
						temps = append(temps, millideg/1000.0)
					}
				}
			}
		}
	}
	if len(temps) == 0 {
		return 0.0, 0.0, false
	}
	var sum float64
	maxVal = -1000.0
	for _, t := range temps {
		sum += t
		if t > maxVal {
			maxVal = t
		}
	}
	return sum / float64(len(temps)), maxVal, true
}

func getLoadAverages(path string) (l1, l5, l15 float64, err error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, 0, 0, err
	}
	fields := strings.Fields(string(data))
	if len(fields) < 3 {
		return 0, 0, 0, fmt.Errorf("invalid loadavg format")
	}
	l1, err = strconv.ParseFloat(fields[0], 64)
	if err != nil {
		return 0, 0, 0, err
	}
	l5, err = strconv.ParseFloat(fields[1], 64)
	if err != nil {
		return 0, 0, 0, err
	}
	l15, err = strconv.ParseFloat(fields[2], 64)
	if err != nil {
		return 0, 0, 0, err
	}
	return l1, l5, l15, nil
}

func (c *CPUCollector) getPowerWatts() float64 {
	data, err := os.ReadFile(c.powercapPath)
	if err != nil {
		return 0.0
	}
	uj, err := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
	if err != nil {
		return 0.0
	}

	now := time.Now()
	if c.prevPowerEnergy == 0 {
		c.prevPowerEnergy = uj
		c.prevPowerTime = now
		return 0.0
	}

	deltaEnergy := uj - c.prevPowerEnergy
	deltaTime := now.Sub(c.prevPowerTime)

	c.prevPowerEnergy = uj
	c.prevPowerTime = now

	if deltaTime.Seconds() > 0 {
		return float64(deltaEnergy) / 1000000.0 / deltaTime.Seconds()
	}
	return 0.0
}
