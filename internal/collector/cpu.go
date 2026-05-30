package collector

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"sort"
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

// PerCPUCore represents usage and frequency details of a single logical CPU.
type PerCPUCore struct {
	CPU          string   `json:"cpu"`
	UsagePercent float64  `json:"usage_percent"`
	FrequencyMHz *float64 `json:"frequency_mhz,omitempty"`
}

// CPUCollector monitors CPU usage, core frequencies, load averages, temperature, and power.
type CPUCollector struct {
	mu              sync.Mutex
	procStatPath    string
	loadavgPath     string
	prevStates      map[string]cpuRawState
	prevPowerEnergy uint64
	prevPowerTime   time.Time
	hasPrev         bool
	warnedRAPL      bool
	warnedTemp      bool
	warnedFreq      bool
}

// NewCPUCollector constructs a CPUCollector with default paths.
func NewCPUCollector() *CPUCollector {
	return &CPUCollector{
		procStatPath: "/proc/stat",
		loadavgPath:  "/proc/loadavg",
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
		usagePercent = round((float64(deltaActive)/float64(deltaTotal))*100, 2)
		userPercent = round((float64((globalCur.user-globalPrev.user)+(globalCur.nice-globalPrev.nice))/float64(deltaTotal))*100, 2)
		sysPercent = round((float64((globalCur.system-globalPrev.system)+(globalCur.irq-globalPrev.irq)+(globalCur.softirq-globalPrev.softirq))/float64(deltaTotal))*100, 2)
		idlePercent = round((float64(globalCur.idle-globalPrev.idle)/float64(deltaTotal))*100, 2)
		iowaitPercent = round((float64(globalCur.iowait-globalPrev.iowait)/float64(deltaTotal))*100, 2)
		stealPercent = round((float64(globalCur.steal-globalPrev.steal)/float64(deltaTotal))*100, 2)
	}

	l1, l5, l15, _ := getLoadAverages(c.loadavgPath)
	freqs := c.getCPUFrequencies()

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
		freqAvg = round(freqSum/float64(len(freqs)), 2)
		freqMin = round(freqMin, 2)
		freqMax = round(freqMax, 2)
	} else {
		freqMin = 0.0
	}

	tempAvg, tempMax, tempOk := c.getCPUTemperatures()
	power := c.getPowerWatts()

	var perCPUMetrics []PerCPUCore
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
			cpuUsage = round((float64(dTotal-dIdleTime)/float64(dTotal))*100, 2)
		}

		cpuInfo := PerCPUCore{
			CPU:          name,
			UsagePercent: cpuUsage,
		}
		if freq, found := freqs[name]; found {
			fVal := round(freq, 2)
			cpuInfo.FrequencyMHz = &fVal
		}
		perCPUMetrics = append(perCPUMetrics, cpuInfo)
	}

	sort.Slice(perCPUMetrics, func(i, j int) bool {
		idxI, _ := strconv.Atoi(strings.TrimPrefix(perCPUMetrics[i].CPU, "cpu"))
		idxJ, _ := strconv.Atoi(strings.TrimPrefix(perCPUMetrics[j].CPU, "cpu"))
		return idxI < idxJ
	})

	c.prevStates = currentStates

	numCPUs := float64(len(perCPUMetrics))
	if numCPUs == 0 {
		numCPUs = 1
	}

	data := map[string]any{
		"usage_percent":       usagePercent,
		"user_percent":        userPercent,
		"system_percent":      sysPercent,
		"idle_percent":        idlePercent,
		"iowait_percent":      iowaitPercent,
		"steal_percent":       stealPercent,
		"load_average_1m":     round(l1, 2),
		"load_average_5m":     round(l5, 2),
		"load_average_15m":    round(l15, 2),
		"normalized_load_1m":  round(l1/numCPUs, 2),
		"normalized_load_5m":  round(l5/numCPUs, 2),
		"normalized_load_15m": round(l15/numCPUs, 2),
		"per_cpu":             perCPUMetrics,
	}

	if len(freqs) > 0 {
		data["frequency_mhz_avg"] = freqAvg
		data["frequency_mhz_min"] = freqMin
		data["frequency_mhz_max"] = freqMax
	}
	if tempOk {
		data["temperature_celsius_avg"] = round(tempAvg, 1)
		data["temperature_celsius_max"] = round(tempMax, 1)
	}
	if power > 0 {
		data["power_watts"] = round(power, 2)
	}

	return []Event{
		{
			Event:     "metric_sample",
			Collector: "cpu",
			Component: "collector",
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

func (c *CPUCollector) getCPUFrequencies() map[string]float64 {
	freqs := make(map[string]float64)

	matches, err := filepath.Glob("/sys/devices/system/cpu/cpu[0-9]*")
	if err == nil && len(matches) > 0 {
		for _, m := range matches {
			cpuName := filepath.Base(m)
			paths := []string{
				filepath.Join(m, "cpufreq/scaling_cur_freq"),
				filepath.Join(m, "cpufreq/cpuinfo_cur_freq"),
				filepath.Join(m, "cpufreq/scaling_max_freq"),
			}
			for _, p := range paths {
				if data, err := os.ReadFile(p); err == nil {
					if val, err := strconv.ParseFloat(strings.TrimSpace(string(data)), 64); err == nil && val > 0 {
						freqs[cpuName] = val / 1000.0
						break
					}
				}
			}
		}
	}

	if len(freqs) == 0 {
		policies, err := filepath.Glob("/sys/devices/system/cpu/cpufreq/policy*")
		if err == nil && len(policies) > 0 {
			for _, p := range policies {
				affectedData, err := os.ReadFile(filepath.Join(p, "affected_cpus"))
				if err == nil {
					cpus := strings.Fields(string(affectedData))
					var freqVal float64
					paths := []string{
						filepath.Join(p, "scaling_cur_freq"),
						filepath.Join(p, "cpuinfo_cur_freq"),
						filepath.Join(p, "scaling_max_freq"),
					}
					for _, fPath := range paths {
						if data, err := os.ReadFile(fPath); err == nil {
							if val, err := strconv.ParseFloat(strings.TrimSpace(string(data)), 64); err == nil && val > 0 {
								freqVal = val / 1000.0
								break
							}
						}
					}
					if freqVal > 0 {
						for _, cpuIdx := range cpus {
							freqs["cpu"+cpuIdx] = freqVal
						}
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
						if mhz, err := strconv.ParseFloat(strings.TrimSpace(parts[1]), 64); err == nil && mhz > 0 {
							freqs[fmt.Sprintf("cpu%d", cpuIdx)] = mhz
							cpuIdx++
						}
					}
				}
			}
			_ = scanner.Err()
		}
	}

	if len(freqs) == 0 {
		if !c.warnedFreq {
			slog.Warn("unable to read CPU frequency metrics",
				slog.String("component", "collector"),
				slog.String("collector", "cpu"),
			)
			c.warnedFreq = true
		}
	}

	return freqs
}

func (c *CPUCollector) getCPUTemperatures() (avgVal, maxVal float64, ok bool) {
	var temps []float64

	matches, err := filepath.Glob("/sys/class/hwmon/hwmon*/name")
	if err == nil && len(matches) > 0 {
		for _, m := range matches {
			if nameData, err := os.ReadFile(m); err == nil {
				name := strings.TrimSpace(string(nameData))
				dir := filepath.Dir(m)
				inputs, err := filepath.Glob(filepath.Join(dir, "temp*_input"))
				if err == nil {
					for _, input := range inputs {
						if c.isCPUTempSensor(input, name) {
							if tempData, err := os.ReadFile(input); err == nil {
								if millideg, err := strconv.ParseFloat(strings.TrimSpace(string(tempData)), 64); err == nil && millideg > 0 {
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
				dir := filepath.Dir(m)
				typePath := filepath.Join(dir, "type")
				useZone := true
				if typeData, err := os.ReadFile(typePath); err == nil {
					zoneType := strings.ToLower(strings.TrimSpace(string(typeData)))
					if !strings.Contains(zoneType, "cpu") &&
						!strings.Contains(zoneType, "package") &&
						!strings.Contains(zoneType, "core") &&
						!strings.Contains(zoneType, "acpitz") &&
						!strings.Contains(zoneType, "soc") &&
						!strings.Contains(zoneType, "x86_pkg") {
						useZone = false
					}
				}
				if useZone {
					if tempData, err := os.ReadFile(m); err == nil {
						if millideg, err := strconv.ParseFloat(strings.TrimSpace(string(tempData)), 64); err == nil && millideg > 0 {
							temps = append(temps, millideg/1000.0)
						}
					}
				}
			}
		}
	}

	if len(temps) == 0 {
		if !c.warnedTemp {
			slog.Warn("no compatible hardware temperature sensors found",
				slog.String("component", "collector"),
				slog.String("collector", "cpu"),
			)
			c.warnedTemp = true
		}
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

func (c *CPUCollector) isCPUTempSensor(inputPath string, name string) bool {
	nameLower := strings.ToLower(name)
	if nameLower == "coretemp" || nameLower == "k10temp" || nameLower == "acpitz" ||
		nameLower == "zenpower" || nameLower == "fam15h_power" || nameLower == "amd_energy" ||
		nameLower == "cpu_thermal" || nameLower == "soc_thermal" {
		return true
	}

	dir := filepath.Dir(inputPath)
	base := filepath.Base(inputPath)
	if strings.HasPrefix(base, "temp") && strings.HasSuffix(base, "_input") {
		sensorNum := strings.TrimSuffix(strings.TrimPrefix(base, "temp"), "_input")
		labelPath := filepath.Join(dir, "temp"+sensorNum+"_label")
		if labelData, err := os.ReadFile(labelPath); err == nil {
			label := strings.ToLower(strings.TrimSpace(string(labelData)))
			if strings.Contains(label, "core") || strings.Contains(label, "package") ||
				strings.Contains(label, "cpu") || strings.Contains(label, "tdie") ||
				strings.Contains(label, "tctl") {
				return true
			}
		}
	}
	return false
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
	raplPaths, _ := filepath.Glob("/sys/class/powercap/intel-rapl:[0-9]*/energy_uj")
	var totalEnergy uint64
	for _, p := range raplPaths {
		if data, err := os.ReadFile(p); err == nil {
			if val, err := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64); err == nil {
				totalEnergy += val
			}
		}
	}

	if totalEnergy > 0 {
		now := time.Now()
		if c.prevPowerEnergy == 0 {
			c.prevPowerEnergy = totalEnergy
			c.prevPowerTime = now
			return 0.0
		}

		deltaEnergy := totalEnergy - c.prevPowerEnergy
		deltaTime := now.Sub(c.prevPowerTime)

		c.prevPowerEnergy = totalEnergy
		c.prevPowerTime = now

		if deltaTime.Seconds() > 0 {
			return float64(deltaEnergy) / 1000000.0 / deltaTime.Seconds()
		}
		return 0.0
	}

	if hwmonPower := c.getHwmonPower(); hwmonPower > 0 {
		return hwmonPower
	}

	if !c.warnedRAPL {
		slog.Warn("failed to retrieve CPU power metrics",
			slog.String("component", "collector"),
			slog.String("collector", "cpu"),
		)
		c.warnedRAPL = true
	}
	return 0.0
}

func (c *CPUCollector) getHwmonPower() float64 {
	matches, err := filepath.Glob("/sys/class/hwmon/hwmon*/name")
	if err != nil || len(matches) == 0 {
		return 0.0
	}
	for _, nameFile := range matches {
		if nameData, err := os.ReadFile(nameFile); err == nil {
			name := strings.TrimSpace(string(nameData))
			if name == "amd_energy" || name == "zenpower" || name == "fam15h_power" || name == "intel_rapl" {
				dir := filepath.Dir(nameFile)
				powerFiles, err := filepath.Glob(filepath.Join(dir, "power*_input"))
				if err == nil && len(powerFiles) > 0 {
					var totalMicrowatts float64
					for _, pFile := range powerFiles {
						if pData, err := os.ReadFile(pFile); err == nil {
							if uw, err := strconv.ParseFloat(strings.TrimSpace(string(pData)), 64); err == nil {
								totalMicrowatts += uw
							}
						}
					}
					if totalMicrowatts > 0 {
						return totalMicrowatts / 1000000.0
					}
				}
			}
		}
	}
	return 0.0
}
