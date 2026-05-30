package collector

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"unsafe"

	"fathom/internal/config"
)

type rawNetDev struct {
	iface     string
	rxBytes   uint64
	rxPackets uint64
	rxErrors  uint64
	rxDropped uint64
	txBytes   uint64
	txPackets uint64
	txErrors  uint64
	txDropped uint64
}

// InterfaceMetadata represents semi-dynamic properties of a network interface.
type InterfaceMetadata struct {
	Interface  string  `json:"interface"`
	IPv4       *string `json:"ipv4"`
	IPv6       *string `json:"ipv6"`
	Operstate  string  `json:"operstate"`
	Carrier    *int    `json:"carrier"`
	MTUBytes   int     `json:"mtu_bytes"`
	SpeedMbps  *int    `json:"speed_mbps"`
	TxQueueLen *int    `json:"tx_queue_len"`
	Duplex     *string `json:"duplex"`
}

// InterfaceMetrics caches previous values for rate calculation.
type InterfaceMetrics struct {
	rxBytes   uint64
	txBytes   uint64
	rxPackets uint64
	txPackets uint64
	rxErrors  uint64
	txErrors  uint64
	rxDropped uint64
	txDropped uint64
}

// NetworkCollector monitors interface traffic rates, states, and IPs.
type NetworkCollector struct {
	mu             sync.Mutex
	procNetDevPath string
	sysClassNetDir string
	cachedMeta     map[string]InterfaceMetadata
	prevMetrics    map[string]InterfaceMetrics
	prevTime       map[string]time.Time
	hasSnapshot    bool
	config         *config.NetworkConfig
	warnedIPs      map[string]bool
}

// NewNetworkCollector constructs a NetworkCollector with default paths.
func NewNetworkCollector(cfg *config.NetworkConfig) *NetworkCollector {
	return &NetworkCollector{
		procNetDevPath: "/proc/net/dev",
		sysClassNetDir: "/sys/class/net",
		cachedMeta:     make(map[string]InterfaceMetadata),
		prevMetrics:    make(map[string]InterfaceMetrics),
		prevTime:       make(map[string]time.Time),
		config:         cfg,
		warnedIPs:      make(map[string]bool),
	}
}

// Name returns the name of the collector.
func (c *NetworkCollector) Name() string {
	return "network"
}

// UpdateConfig updates the active network filtering configuration block dynamically.
func (c *NetworkCollector) UpdateConfig(cfg *config.NetworkConfig) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.config = cfg
}

// Collect returns metadata snapshots, change events, and dynamic interface traffic rate metrics.
func (c *NetworkCollector) Collect(ctx context.Context) ([]Event, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	netDevStats, err := parseProcNetDev(c.procNetDevPath)
	if err != nil {
		return nil, fmt.Errorf("failed to parse proc/net/dev: %w", err)
	}

	var events []Event
	now := time.Now()

	currentMeta := make(map[string]InterfaceMetadata)
	var sortedIfaces []string

	for iface := range netDevStats {
		operstate, _ := readSysFileString(filepath.Join(c.sysClassNetDir, iface, "operstate"))
		if operstate == "" {
			operstate = "unknown"
		}

		if !c.shouldKeepInterface(iface, operstate) {
			continue
		}

		ipv4, ipv6 := c.getIPsWithFallbacks(iface)
		var carrierPtr *int
		if carrier, err := readSysFileInt(filepath.Join(c.sysClassNetDir, iface, "carrier")); err == nil {
			carrierPtr = &carrier
		}

		mtu := 1500
		if sysIface, err := net.InterfaceByName(iface); err == nil {
			mtu = sysIface.MTU
		}

		var speedPtr *int
		if speed, err := readSysFileInt(filepath.Join(c.sysClassNetDir, iface, "speed")); err == nil && speed > 0 {
			speedPtr = &speed
		}

		var txQueueLenPtr *int
		if txql, err := readSysFileInt(filepath.Join(c.sysClassNetDir, iface, "tx_queue_len")); err == nil {
			txQueueLenPtr = &txql
		}

		var duplexPtr *string
		if duplex, err := readSysFileString(filepath.Join(c.sysClassNetDir, iface, "duplex")); err == nil && duplex != "" {
			duplexPtr = &duplex
		}

		currentMeta[iface] = InterfaceMetadata{
			Interface:  iface,
			IPv4:       ipv4,
			IPv6:       ipv6,
			Operstate:  operstate,
			Carrier:    carrierPtr,
			MTUBytes:   mtu,
			SpeedMbps:  speedPtr,
			TxQueueLen: txQueueLenPtr,
			Duplex:     duplexPtr,
		}
		sortedIfaces = append(sortedIfaces, iface)
	}
	sort.Strings(sortedIfaces)

	if !c.hasSnapshot {
		var interfacesList []any
		for _, iface := range sortedIfaces {
			interfacesList = append(interfacesList, currentMeta[iface])
		}
		events = append(events, Event{
			Event:     "network_metadata_snapshot",
			Collector: "network",
			Component: "collector",
			Data: map[string]any{
				"interfaces": interfacesList,
			},
		})
		c.cachedMeta = currentMeta
		c.hasSnapshot = true
	} else {
		var removedKeys []string
		for iface := range c.cachedMeta {
			if _, exists := currentMeta[iface]; !exists {
				removedKeys = append(removedKeys, iface)
			}
		}
		sort.Strings(removedKeys)

		for _, iface := range removedKeys {
			events = append(events, Event{
				Event:     "network_metadata_changed",
				Collector: "network",
				Component: "collector",
				Data: map[string]any{
					"interface":      iface,
					"old":            c.cachedMeta[iface],
					"new":            nil,
					"changed_fields": []string{"interface_removed"},
				},
			})
		}

		for _, iface := range sortedIfaces {
			newM := currentMeta[iface]
			oldM, exists := c.cachedMeta[iface]
			if !exists {
				events = append(events, Event{
					Event:     "network_metadata_changed",
					Collector: "network",
					Component: "collector",
					Data: map[string]any{
						"interface":      iface,
						"old":            nil,
						"new":            newM,
						"changed_fields": []string{"interface_added"},
					},
				})
			} else if !reflect.DeepEqual(oldM, newM) {
				var changed []string
				if !equalStringPtr(oldM.IPv4, newM.IPv4) {
					changed = append(changed, "ipv4")
				}
				if !equalStringPtr(oldM.IPv6, newM.IPv6) {
					changed = append(changed, "ipv6")
				}
				if oldM.Operstate != newM.Operstate {
					changed = append(changed, "operstate")
				}
				if oldM.Carrier != newM.Carrier {
					changed = append(changed, "carrier")
				}
				if oldM.MTUBytes != newM.MTUBytes {
					changed = append(changed, "mtu_bytes")
				}
				if !equalIntPtr(oldM.SpeedMbps, newM.SpeedMbps) {
					changed = append(changed, "speed_mbps")
				}
				if !equalIntPtr(oldM.TxQueueLen, newM.TxQueueLen) {
					changed = append(changed, "tx_queue_len")
				}
				if !equalStringPtr(oldM.Duplex, newM.Duplex) {
					changed = append(changed, "duplex")
				}

				if len(changed) > 0 {
					events = append(events, Event{
						Event:     "network_metadata_changed",
						Collector: "network",
						Component: "collector",
						Data: map[string]any{
							"interface":      iface,
							"old":            oldM,
							"new":            newM,
							"changed_fields": changed,
						},
					})
				}
			}
		}
		c.cachedMeta = currentMeta
	}

	for _, iface := range sortedIfaces {
		stats := netDevStats[iface]
		var rxRate, txRate, rxPktRate, txPktRate float64
		var rxErrRate, txErrRate, rxDropRate, txDropRate float64

		prev, ok := c.prevMetrics[iface]
		prevT, okT := c.prevTime[iface]

		if ok && okT {
			duration := now.Sub(prevT).Seconds()
			if duration > 0 {
				if stats.rxBytes >= prev.rxBytes {
					rxRate = float64(stats.rxBytes-prev.rxBytes) / duration
				}
				if stats.txBytes >= prev.txBytes {
					txRate = float64(stats.txBytes-prev.txBytes) / duration
				}
				if stats.rxPackets >= prev.rxPackets {
					rxPktRate = float64(stats.rxPackets-prev.rxPackets) / duration
				}
				if stats.txPackets >= prev.txPackets {
					txPktRate = float64(stats.txPackets-prev.txPackets) / duration
				}
				if stats.rxErrors >= prev.rxErrors {
					rxErrRate = float64(stats.rxErrors-prev.rxErrors) / duration
				}
				if stats.txErrors >= prev.txErrors {
					txErrRate = float64(stats.txErrors-prev.txErrors) / duration
				}
				if stats.rxDropped >= prev.rxDropped {
					rxDropRate = float64(stats.rxDropped-prev.rxDropped) / duration
				}
				if stats.txDropped >= prev.txDropped {
					txDropRate = float64(stats.txDropped-prev.txDropped) / duration
				}
			}
		}

		c.prevMetrics[iface] = InterfaceMetrics{
			rxBytes:   stats.rxBytes,
			txBytes:   stats.txBytes,
			rxPackets: stats.rxPackets,
			txPackets: stats.txPackets,
			rxErrors:  stats.rxErrors,
			txErrors:  stats.txErrors,
			rxDropped: stats.rxDropped,
			txDropped: stats.txDropped,
		}
		c.prevTime[iface] = now

		events = append(events, Event{
			Event:     "metric_sample",
			Collector: "network",
			Component: "collector",
			Data: map[string]any{
				"interface":             iface,
				"rx_bytes_total":        stats.rxBytes,
				"tx_bytes_total":        stats.txBytes,
				"rx_packets_total":      stats.rxPackets,
				"tx_packets_total":      stats.txPackets,
				"rx_errors_total":       stats.rxErrors,
				"tx_errors_total":       stats.txErrors,
				"rx_dropped_total":      stats.rxDropped,
				"tx_dropped_total":      stats.txDropped,
				"rx_bytes_per_second":   round(rxRate, 2),
				"tx_bytes_per_second":   round(txRate, 2),
				"rx_packets_per_second": round(rxPktRate, 2),
				"tx_packets_per_second": round(txPktRate, 2),
				"rx_errors_per_second":  round(rxErrRate, 2),
				"tx_errors_per_second":  round(txErrRate, 2),
				"rx_dropped_per_second": round(rxDropRate, 2),
				"tx_dropped_per_second": round(txDropRate, 2),
			},
		})
	}

	return events, nil
}

func (c *NetworkCollector) shouldKeepInterface(iface, operstate string) bool {
	if c.config != nil && len(c.config.IncludeInterfaces) > 0 {
		return slices.Contains(c.config.IncludeInterfaces, iface)
	}

	includeLoopback := false
	if c.config != nil && c.config.IncludeLoopback != nil {
		includeLoopback = *c.config.IncludeLoopback
	}
	if iface == "lo" && !includeLoopback {
		return false
	}

	includeDown := false
	if c.config != nil && c.config.IncludeDown != nil {
		includeDown = *c.config.IncludeDown
	}
	if operstate == "down" && !includeDown {
		return false
	}

	includeVirtual := false
	if c.config != nil && c.config.IncludeVirtual != nil {
		includeVirtual = *c.config.IncludeVirtual
	}
	if !includeVirtual && isVirtualInterface(c.sysClassNetDir, iface) {
		return false
	}

	excludePrefixes := []string{"veth", "br-", "docker", "virbr"}
	if c.config != nil && len(c.config.ExcludePrefixes) > 0 {
		excludePrefixes = c.config.ExcludePrefixes
	}
	for _, prefix := range excludePrefixes {
		if strings.HasPrefix(iface, prefix) {
			return false
		}
	}

	return true
}

func isVirtualInterface(sysClassNetDir, iface string) bool {
	_, err := os.Stat(filepath.Join(sysClassNetDir, iface, "device"))
	return os.IsNotExist(err)
}

func parseProcNetDev(path string) (map[string]rawNetDev, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	res := make(map[string]rawNetDev)
	scanner := bufio.NewScanner(file)
	lineCount := 0
	for scanner.Scan() {
		lineCount++
		if lineCount <= 2 {
			continue
		}
		line := scanner.Text()
		parts := strings.SplitN(line, ":", 2)
		if len(parts) < 2 {
			continue
		}
		iface := strings.TrimSpace(parts[0])

		fields := strings.Fields(parts[1])
		if len(fields) < 16 {
			continue
		}

		rxB, _ := strconv.ParseUint(fields[0], 10, 64)
		rxP, _ := strconv.ParseUint(fields[1], 10, 64)
		rxE, _ := strconv.ParseUint(fields[2], 10, 64)
		rxD, _ := strconv.ParseUint(fields[3], 10, 64)

		txB, _ := strconv.ParseUint(fields[8], 10, 64)
		txP, _ := strconv.ParseUint(fields[9], 10, 64)
		txE, _ := strconv.ParseUint(fields[10], 10, 64)
		txD, _ := strconv.ParseUint(fields[11], 10, 64)

		res[iface] = rawNetDev{
			iface:     iface,
			rxBytes:   rxB,
			rxPackets: rxP,
			rxErrors:  rxE,
			rxDropped: rxD,
			txBytes:   txB,
			txPackets: txP,
			txErrors:  txE,
			txDropped: txD,
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return res, nil
}

func getIPs(ifaceName string) (ipv4, ipv6 *string, err error) {
	iface, err := net.InterfaceByName(ifaceName)
	if err != nil {
		return nil, nil, err
	}
	addrs, err := iface.Addrs()
	if err != nil {
		return nil, nil, err
	}
	for _, addr := range addrs {
		ipNet, ok := addr.(*net.IPNet)
		if !ok {
			continue
		}
		ip := ipNet.IP
		if ip4 := ip.To4(); ip4 != nil {
			s := ip4.String()
			ipv4 = &s
		} else if ip16 := ip.To16(); ip16 != nil {
			s := ip16.String()
			ipv6 = &s
		}
	}
	return ipv4, ipv6, nil
}

func readSysFileInt(path string) (int, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	val, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		return 0, err
	}
	return val, nil
}

func readSysFileString(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(data)), nil
}

func equalStringPtr(a, b *string) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}

func equalIntPtr(a, b *int) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}

func (c *NetworkCollector) getIPsWithFallbacks(ifaceName string) (ipv4, ipv6 *string) {
	var errs []string

	// Method 1: standard net.InterfaceByName
	ip4, ip6, err := getIPs(ifaceName)
	if err == nil {
		return ip4, ip6
	}
	errs = append(errs, fmt.Sprintf("net.InterfaceByName: %v", err))

	// Method 2: standard net.Interfaces list search
	ip4, ip6, err = getIPsFallbackList(ifaceName)
	if err == nil {
		return ip4, ip6
	}
	errs = append(errs, fmt.Sprintf("net.Interfaces list: %v", err))

	// Method 3: Low-level /proc and ioctl fallbacks
	var fallbackIPv4, fallbackIPv6 *string

	// Try ioctl for IPv4
	ip4Str, err := getIPv4ViaIoctl(ifaceName)
	if err == nil {
		fallbackIPv4 = &ip4Str
	} else {
		errs = append(errs, fmt.Sprintf("ioctl SIOCGIFADDR: %v", err))
	}

	// Try procfs for IPv6
	ip6Str, err := getIPv6FromProc(ifaceName)
	if err == nil {
		fallbackIPv6 = &ip6Str
	} else {
		errs = append(errs, fmt.Sprintf("procfs if_inet6: %v", err))
	}

	if fallbackIPv4 != nil || fallbackIPv6 != nil {
		return fallbackIPv4, fallbackIPv6
	}

	if c.warnedIPs == nil {
		c.warnedIPs = make(map[string]bool)
	}
	alreadyWarned := c.warnedIPs[ifaceName]
	c.warnedIPs[ifaceName] = true

	if !alreadyWarned {
		slog.Warn("failed to retrieve IP addresses for interface",
			slog.String("component", "collector"),
			slog.String("collector", "network"),
			slog.String("interface", ifaceName),
			slog.String("errors", strings.Join(errs, "; ")),
		)
	}

	return nil, nil
}

func getIPsFallbackList(ifaceName string) (ipv4, ipv6 *string, err error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil, nil, err
	}
	for _, iface := range ifaces {
		if iface.Name == ifaceName {
			addrs, err := iface.Addrs()
			if err != nil {
				return nil, nil, err
			}
			for _, addr := range addrs {
				ipNet, ok := addr.(*net.IPNet)
				if !ok {
					continue
				}
				ip := ipNet.IP
				if ip4 := ip.To4(); ip4 != nil {
					s := ip4.String()
					ipv4 = &s
				} else if ip16 := ip.To16(); ip16 != nil {
					s := ip16.String()
					ipv6 = &s
				}
			}
			return ipv4, ipv6, nil
		}
	}
	return nil, nil, fmt.Errorf("interface %s not found in net.Interfaces()", ifaceName)
}

func getIPv4ViaIoctl(ifaceName string) (string, error) {
	fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_DGRAM, syscall.IPPROTO_IP)
	if err != nil {
		return "", err
	}
	defer syscall.Close(fd)

	var ifr [40]byte
	copy(ifr[:15], ifaceName)

	_, _, errno := syscall.Syscall(
		syscall.SYS_IOCTL,
		uintptr(fd),
		uintptr(syscall.SIOCGIFADDR),
		uintptr(unsafe.Pointer(&ifr[0])),
	)
	if errno != 0 {
		return "", errno
	}

	ip := net.IPv4(ifr[20], ifr[21], ifr[22], ifr[23])
	return ip.String(), nil
}

func getIPv6FromProc(ifaceName string) (string, error) {
	file, err := os.Open("/proc/net/if_inet6")
	if err != nil {
		return "", err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Fields(line)
		if len(fields) < 6 {
			continue
		}

		if fields[5] != ifaceName {
			continue
		}

		hexIP := fields[0]
		if len(hexIP) != 32 {
			continue
		}

		ipBytes := make([]byte, 16)
		for i := range 16 {
			b, err := strconv.ParseUint(hexIP[i*2:i*2+2], 16, 8)
			if err != nil {
				return "", err
			}
			ipBytes[i] = byte(b)
		}

		ip := net.IP(ipBytes)
		return ip.String(), nil
	}

	if err := scanner.Err(); err != nil {
		return "", fmt.Errorf("failed to scan /proc/net/if_inet6: %w", err)
	}

	return "", fmt.Errorf("interface %s not found in /proc/net/if_inet6", ifaceName)
}
