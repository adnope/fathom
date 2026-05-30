package collector

import (
	"bufio"
	"os"
	"runtime"
	"strings"
	"syscall"
)

// GetHostMetadata retrieves static details about the Linux system.
func GetHostMetadata() Event {
	data := map[string]any{
		"os":                 "linux",
		"hostname":           "unknown",
		"kernel":             "unknown",
		"arch":               runtime.GOARCH,
		"cpu_count_logical":  runtime.NumCPU(),
		"cpu_count_physical": 1,
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

	if physicalCPUs := getPhysicalCPUCount(); physicalCPUs > 0 {
		data["cpu_count_physical"] = physicalCPUs
	}

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
		// Fallback gracefully on scanner error
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

func getPhysicalCPUCount() int {
	file, err := os.Open("/proc/cpuinfo")
	if err != nil {
		return 1
	}
	defer file.Close()

	physicalIDs := make(map[string]bool)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "physical id") {
			parts := strings.SplitN(line, ":", 2)
			if len(parts) == 2 {
				physicalIDs[strings.TrimSpace(parts[1])] = true
			}
		}
	}
	if err := scanner.Err(); err != nil {
		// Graceful fallback to 1 physical CPU if scanning fails
		return 1
	}
	if len(physicalIDs) > 0 {
		return len(physicalIDs)
	}
	return 1
}
