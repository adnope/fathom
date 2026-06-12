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

type cpuUsageBreakdown struct {
	usagePercent  float64
	userPercent   float64
	sysPercent    float64
	idlePercent   float64
	iowaitPercent float64
	stealPercent  float64
}

// PerCPUCore represents usage and frequency details of a single logical CPU.
type PerCPUCore struct {
	CPU          string   `json:"cpu"`
	UsagePercent float64  `json:"usage_percent"`
	FrequencyMHz *float64 `json:"frequency_mhz"`
}

// CPUCollector monitors CPU usage, core frequencies, load averages, temperature, and power.
type CPUCollector struct {
	mu              sync.Mutex
	procStatPath    string
	loadavgPath     string
	procCPUInfoPath string
	sysCPUPath      string
	sysHwmonPath    string
	sysThermalPath  string
	powercapPath    string
	prevStates      map[string]cpuRawState
	prevSampleTime  time.Time
	prevPowerEnergy uint64
	prevPowerTime   time.Time
	now             func() time.Time
	hasPrev         bool
	issues          *collectorIssueLogger
}

// NewCPUCollector constructs a CPUCollector with default paths.
func NewCPUCollector() *CPUCollector {
	return &CPUCollector{
		procStatPath:    "/proc/stat",
		loadavgPath:     "/proc/loadavg",
		procCPUInfoPath: "/proc/cpuinfo",
		sysCPUPath:      "/sys/devices/system/cpu",
		sysHwmonPath:    "/sys/class/hwmon",
		sysThermalPath:  "/sys/class/thermal",
		powercapPath:    "/sys/class/powercap",
		prevStates:      make(map[string]cpuRawState),
		issues:          newCollectorIssueLogger(),
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
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	previousSampleTime := c.prevSampleTime
	sampleStart := c.currentTime()
	currentStates, err := parseProcStat(c.procStatPath)
	if err != nil {
		return nil, fmt.Errorf("read required cpu source %s: %w", c.procStatPath, err)
	}

	if !c.hasPrev {
		c.prevStates = currentStates
		c.hasPrev = true
		c.prevSampleTime = sampleStart
		previousSampleTime = sampleStart

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}

		currentStates, err = parseProcStat(c.procStatPath)
		if err != nil {
			return nil, fmt.Errorf("read required cpu source %s for delta: %w", c.procStatPath, err)
		}
	}
	sampleTime := c.currentTime()
	sampleInterval := sampleIntervalSeconds(previousSampleTime, sampleTime)

	globalCur, ok := currentStates["cpu"]
	if !ok {
		return nil, fmt.Errorf("missing global cpu state in /proc/stat")
	}
	globalPrev, ok := c.prevStates["cpu"]
	if !ok {
		globalPrev = globalCur
	}
	if globalCur.total() < globalPrev.total() || globalCur.idleTime() < globalPrev.idleTime() {
		c.issueLogger().log(slog.LevelDebug, c.Name(), actionZeroMetric, "cpu_usage_percent", c.procStatPath, nil)
	} else {
		c.issueLogger().clear(c.Name(), actionZeroMetric, "cpu_usage_percent", c.procStatPath)
	}

	usage := calculateCPUUsage(globalCur, globalPrev)

	l1, l5, l15, loadOk := c.loadAverages()
	freqs := c.getCPUFrequencies()
	freqAvg, freqMin, freqMax, hasFreqs := summarizeCPUFrequencies(freqs)

	tempAvg, tempMax, tempOk := c.getCPUTemperatures()
	power := c.getPowerWatts()
	perCPUMetrics := buildPerCPUMetrics(currentStates, c.prevStates, freqs)

	c.prevStates = currentStates
	c.prevSampleTime = sampleTime
	data := buildCPUMetricData(usage, l1, l5, l15, loadOk, perCPUMetrics, freqAvg, freqMin, freqMax, hasFreqs, tempAvg, tempMax, tempOk, power, sampleInterval)

	return []Event{
		{
			Event:     eventMetricSample,
			Collector: c.Name(),
			Component: componentCollector,
			Data:      data,
		},
	}, nil
}

func (c *CPUCollector) issueLogger() *collectorIssueLogger {
	if c.issues == nil {
		c.issues = newCollectorIssueLogger()
	}
	return c.issues
}

func (c *CPUCollector) currentTime() time.Time {
	if c.now != nil {
		return c.now()
	}
	return time.Now()
}

func calculateCPUUsage(cur, prev cpuRawState) cpuUsageBreakdown {
	deltaTotal, ok := nonNegativeDelta(cur.total(), prev.total())
	if !ok {
		return cpuUsageBreakdown{}
	}
	deltaIdleTime, ok := nonNegativeDelta(cur.idleTime(), prev.idleTime())
	if !ok || deltaIdleTime > deltaTotal {
		return cpuUsageBreakdown{}
	}
	deltaActive := deltaTotal - deltaIdleTime

	var usage cpuUsageBreakdown
	if deltaTotal > 0 {
		usage.usagePercent = round((float64(deltaActive)/float64(deltaTotal))*100, 2)
		usage.userPercent = round((float64(safeCPUCounterDelta(cur.user, prev.user)+safeCPUCounterDelta(cur.nice, prev.nice))/float64(deltaTotal))*100, 2)
		usage.sysPercent = round((float64(safeCPUCounterDelta(cur.system, prev.system)+safeCPUCounterDelta(cur.irq, prev.irq)+safeCPUCounterDelta(cur.softirq, prev.softirq))/float64(deltaTotal))*100, 2)
		usage.idlePercent = round((float64(safeCPUCounterDelta(cur.idle, prev.idle))/float64(deltaTotal))*100, 2)
		usage.iowaitPercent = round((float64(safeCPUCounterDelta(cur.iowait, prev.iowait))/float64(deltaTotal))*100, 2)
		usage.stealPercent = round((float64(safeCPUCounterDelta(cur.steal, prev.steal))/float64(deltaTotal))*100, 2)
	}

	return usage
}

func safeCPUCounterDelta(curr, prev uint64) uint64 {
	delta, ok := nonNegativeDelta(curr, prev)
	if !ok {
		return 0
	}
	return delta
}

func summarizeCPUFrequencies(freqs map[string]float64) (avg, min, max float64, ok bool) {
	if len(freqs) == 0 {
		return 0, 0, 0, false
	}

	min = math.MaxFloat64
	for _, freq := range freqs {
		avg += freq
		if freq < min {
			min = freq
		}
		if freq > max {
			max = freq
		}
	}

	return round(avg/float64(len(freqs)), 2), round(min, 2), round(max, 2), true
}

func buildPerCPUMetrics(currentStates, prevStates map[string]cpuRawState, freqs map[string]float64) []PerCPUCore {
	perCPUMetrics := make([]PerCPUCore, 0, len(currentStates))
	for name, cur := range currentStates {
		if name == "cpu" {
			continue
		}

		prev, ok := prevStates[name]
		if !ok {
			prev = cur
		}

		cpuInfo := PerCPUCore{
			CPU:          name,
			UsagePercent: calculateCPUUsage(cur, prev).usagePercent,
		}
		if freq, found := freqs[name]; found {
			freqMHz := round(freq, 2)
			cpuInfo.FrequencyMHz = &freqMHz
		}
		perCPUMetrics = append(perCPUMetrics, cpuInfo)
	}

	sort.Slice(perCPUMetrics, func(i, j int) bool {
		idxI, _ := strconv.Atoi(strings.TrimPrefix(perCPUMetrics[i].CPU, "cpu"))
		idxJ, _ := strconv.Atoi(strings.TrimPrefix(perCPUMetrics[j].CPU, "cpu"))
		return idxI < idxJ
	})

	return perCPUMetrics
}

func buildCPUMetricData(usage cpuUsageBreakdown, l1, l5, l15 float64, loadOk bool, perCPUMetrics []PerCPUCore, freqAvg, freqMin, freqMax float64, hasFreqs bool, tempAvg, tempMax float64, tempOk bool, power float64, sampleIntervalSeconds any) map[string]any {
	numCPUs := float64(len(perCPUMetrics))
	if numCPUs == 0 {
		numCPUs = 1
	}

	data := map[string]any{
		"sample_interval_seconds":     sampleIntervalSeconds,
		"cpu_usage_percent":           usage.usagePercent,
		"cpu_user_percent":            usage.userPercent,
		"cpu_system_percent":          usage.sysPercent,
		"cpu_idle_percent":            usage.idlePercent,
		"cpu_iowait_percent":          usage.iowaitPercent,
		"cpu_steal_percent":           usage.stealPercent,
		"cpu_load_average_1m":         nil,
		"cpu_load_average_5m":         nil,
		"cpu_load_average_15m":        nil,
		"cpu_normalized_load_1m":      nil,
		"cpu_normalized_load_5m":      nil,
		"cpu_normalized_load_15m":     nil,
		"cpu_frequency_mhz_avg":       nil,
		"cpu_frequency_mhz_min":       nil,
		"cpu_frequency_mhz_max":       nil,
		"cpu_temperature_celsius_avg": nil,
		"cpu_temperature_celsius_max": nil,
		"cpu_power_watts":             nil,
		"per_cpu":                     perCPUMetrics,
	}

	if loadOk {
		data["cpu_load_average_1m"] = round(l1, 2)
		data["cpu_load_average_5m"] = round(l5, 2)
		data["cpu_load_average_15m"] = round(l15, 2)
		data["cpu_normalized_load_1m"] = round(l1/numCPUs, 2)
		data["cpu_normalized_load_5m"] = round(l5/numCPUs, 2)
		data["cpu_normalized_load_15m"] = round(l15/numCPUs, 2)
	}

	if hasFreqs {
		data["cpu_frequency_mhz_avg"] = freqAvg
		data["cpu_frequency_mhz_min"] = freqMin
		data["cpu_frequency_mhz_max"] = freqMax
	}
	if tempOk {
		data["cpu_temperature_celsius_avg"] = round(tempAvg, 1)
		data["cpu_temperature_celsius_max"] = round(tempMax, 1)
	}
	if power > 0 {
		data["cpu_power_watts"] = round(power, 2)
	}

	return data
}

func sampleIntervalSeconds(previous, current time.Time) any {
	if previous.IsZero() {
		return nil
	}
	duration := current.Sub(previous).Seconds()
	if duration <= 0 {
		return nil
	}
	return round(duration, 3)
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
			validLine := true
			for i := range 8 {
				v, err := strconv.ParseUint(fields[i+1], 10, 64)
				if err != nil {
					if name == "cpu" {
						return nil, fmt.Errorf("parse aggregate cpu field %d: %w", i+1, err)
					}
					validLine = false
					break
				}
				vals[i] = v
			}
			if !validLine {
				continue
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
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if _, ok := states["cpu"]; !ok {
		return nil, fmt.Errorf("missing aggregate cpu line")
	}
	return states, nil
}

func (c *CPUCollector) getCPUFrequencies() map[string]float64 {
	freqs := make(map[string]float64)
	sysCPUPath := c.sysCPUPath
	if sysCPUPath == "" {
		sysCPUPath = "/sys/devices/system/cpu"
	}
	procCPUInfoPath := c.procCPUInfoPath
	if procCPUInfoPath == "" {
		procCPUInfoPath = "/proc/cpuinfo"
	}

	matches, err := filepath.Glob(filepath.Join(sysCPUPath, "cpu[0-9]*"))
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
		policies, err := filepath.Glob(filepath.Join(sysCPUPath, "cpufreq/policy*"))
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
		if file, err := os.Open(procCPUInfoPath); err == nil {
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
		c.issueLogger().log(slog.LevelDebug, c.Name(), actionOmitMetric, metricFrequencyMHz, sysCPUPath, nil)
	} else {
		c.issueLogger().clear(c.Name(), actionOmitMetric, metricFrequencyMHz, sysCPUPath)
	}

	return freqs
}

func (c *CPUCollector) getCPUTemperatures() (avgVal, maxVal float64, ok bool) {
	var temps []float64
	sysHwmonPath := c.sysHwmonPath
	if sysHwmonPath == "" {
		sysHwmonPath = "/sys/class/hwmon"
	}
	sysThermalPath := c.sysThermalPath
	if sysThermalPath == "" {
		sysThermalPath = "/sys/class/thermal"
	}

	matches, err := filepath.Glob(filepath.Join(sysHwmonPath, "hwmon*/name"))
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
		matches, err := filepath.Glob(filepath.Join(sysThermalPath, "thermal_zone*/temp"))
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
		c.issueLogger().log(slog.LevelDebug, c.Name(), actionOmitMetric, metricTemperatureCelsius, sysHwmonPath, nil)
		return 0.0, 0.0, false
	}
	c.issueLogger().clear(c.Name(), actionOmitMetric, metricTemperatureCelsius, sysHwmonPath)

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

func (c *CPUCollector) loadAverages() (l1, l5, l15 float64, ok bool) {
	l1, l5, l15, err := getLoadAverages(c.loadavgPath)
	if err != nil {
		c.issueLogger().log(slog.LevelDebug, c.Name(), actionOmitMetric, "cpu_load_average", c.loadavgPath, err)
		return 0, 0, 0, false
	}
	c.issueLogger().clear(c.Name(), actionOmitMetric, "cpu_load_average", c.loadavgPath)
	return l1, l5, l15, true
}

func (c *CPUCollector) getPowerWatts() float64 {
	powercapPath := c.powercapPath
	if powercapPath == "" {
		powercapPath = "/sys/class/powercap"
	}
	raplPaths, _ := filepath.Glob(filepath.Join(powercapPath, "intel-rapl:[0-9]*/energy_uj"))
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
			c.issueLogger().clear(c.Name(), actionOmitMetric, metricPowerWatts, powercapPath)
			return 0.0
		}

		deltaEnergy, ok := nonNegativeDelta(totalEnergy, c.prevPowerEnergy)
		deltaTime := now.Sub(c.prevPowerTime)

		c.prevPowerEnergy = totalEnergy
		c.prevPowerTime = now

		if !ok {
			c.issueLogger().log(slog.LevelDebug, c.Name(), actionZeroMetric, metricPowerWatts, powercapPath, nil)
			return 0.0
		}
		if deltaTime.Seconds() > 0 {
			c.issueLogger().clear(c.Name(), actionOmitMetric, metricPowerWatts, powercapPath)
			c.issueLogger().clear(c.Name(), actionZeroMetric, metricPowerWatts, powercapPath)
			return float64(deltaEnergy) / 1000000.0 / deltaTime.Seconds()
		}
		return 0.0
	}

	if hwmonPower := c.getHwmonPower(); hwmonPower > 0 {
		c.issueLogger().clear(c.Name(), actionOmitMetric, metricPowerWatts, powercapPath)
		return hwmonPower
	}

	c.issueLogger().log(slog.LevelDebug, c.Name(), actionOmitMetric, metricPowerWatts, powercapPath, nil)
	return 0.0
}

func (c *CPUCollector) getHwmonPower() float64 {
	sysHwmonPath := c.sysHwmonPath
	if sysHwmonPath == "" {
		sysHwmonPath = "/sys/class/hwmon"
	}
	matches, err := filepath.Glob(filepath.Join(sysHwmonPath, "hwmon*/name"))
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
