package collector

import (
	"bufio"
	"log/slog"
	"os"
	"runtime"
	"strconv"
	"strings"
	"syscall"
)

// GetHostMetadata retrieves static details about the Linux system.
func GetHostMetadata() Event {
	data := map[string]any{
		"os":                            "linux",
		"hostname":                      "unknown",
		"kernel":                        "unknown",
		"arch":                          runtime.GOARCH,
		"cpu_count_logical":             runtime.NumCPU(),
		"cpu_count_physical":            runtime.NumCPU(),
		"cpu_count_sockets":             1,
		"system_boot_time_unix_seconds": getBootTime(),
	}

	if hostname, err := os.Hostname(); err == nil {
		data["hostname"] = hostname
	}

	if kernel, err := os.ReadFile("/proc/sys/kernel/osrelease"); err == nil {
		data["kernel"] = strings.TrimSpace(string(kernel))
	}

	if osRelease := getOSRelease(); osRelease != "" {
		data["os_release"] = osRelease
	}

	if arch := getArch(); arch != "" {
		data["arch"] = arch
	}

	sockets, physicalCores := getCPUTopology()
	data["cpu_count_sockets"] = sockets
	data["cpu_count_physical"] = physicalCores

	return Event{
		Event:     "host_metadata",
		Component: "agent",
		Data:      data,
	}
}

func getOSRelease() string {
	file, err := os.Open("/etc/os-release")
	if err != nil {
		return "linux"
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if after, ok := strings.CutPrefix(line, "PRETTY_NAME="); ok {
			val := after
			return strings.Trim(val, `"'`)
		}
	}
	if err := scanner.Err(); err != nil {
		return "linux"
	}
	return "linux"
}

func getArch() string {
	var uts syscall.Utsname
	if err := syscall.Uname(&uts); err != nil {
		return runtime.GOARCH
	}
	var buf []byte
	for _, b := range uts.Machine {
		if b == 0 {
			break
		}
		buf = append(buf, byte(b))
	}
	return string(buf)
}

func getCPUTopology() (sockets, physicalCores int) {
	file, err := os.Open("/proc/cpuinfo")
	if err != nil {
		return 1, runtime.NumCPU()
	}
	defer file.Close()

	socketsMap := make(map[string]bool)
	coresMap := make(map[string]bool)

	var currentPhysID string
	var currentCoreID string

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.TrimSpace(line) == "" {
			if currentPhysID != "" {
				socketsMap[currentPhysID] = true
				if currentCoreID != "" {
					coresMap[currentPhysID+":"+currentCoreID] = true
				}
			}
			currentPhysID = ""
			currentCoreID = ""
			continue
		}

		parts := strings.SplitN(line, ":", 2)
		if len(parts) < 2 {
			continue
		}
		key := strings.TrimSpace(parts[0])
		val := strings.TrimSpace(parts[1])

		switch key {
		case "physical id":
			currentPhysID = val
		case "core id":
			currentCoreID = val
		}
	}
	if currentPhysID != "" {
		socketsMap[currentPhysID] = true
		if currentCoreID != "" {
			coresMap[currentPhysID+":"+currentCoreID] = true
		}
	}

	if err := scanner.Err(); err != nil {
		// Fallback gracefully on scanner error
	}

	sockets = len(socketsMap)
	physicalCores = len(coresMap)

	if sockets == 0 {
		sockets = 1
	}
	if physicalCores == 0 {
		physicalCores = runtime.NumCPU()
	}
	return sockets, physicalCores
}

func getBootTime() uint64 {
	file, err := os.Open("/proc/stat")
	if err != nil {
		return 0
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "btime ") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				if val, err := strconv.ParseUint(fields[1], 10, 64); err == nil {
					return val
				}
			}
		}
	}
	if err := scanner.Err(); err != nil {
		slog.Error("failed to scan /proc/stat for boot time", slog.String("error", err.Error()))
	}
	return 0
}
