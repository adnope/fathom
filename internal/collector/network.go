package collector

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

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
	Interface string  `json:"interface"`
	IPv4      *string `json:"ipv4"`
	IPv6      *string `json:"ipv6"`
	Operstate string  `json:"operstate"`
	Carrier   int     `json:"carrier"`
	MTUBytes  int     `json:"mtu_bytes"`
	SpeedMbps *int    `json:"speed_mbps"`
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

	// 1. Gather current metadata for matching interfaces
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

		ipv4, ipv6 := getIPs(iface)
		carrier, _ := readSysFileInt(filepath.Join(c.sysClassNetDir, iface, "carrier"))

		mtu := 1500
		if sysIface, err := net.InterfaceByName(iface); err == nil {
			mtu = sysIface.MTU
		}

		var speedPtr *int
		if speed, err := readSysFileInt(filepath.Join(c.sysClassNetDir, iface, "speed")); err == nil && speed > 0 {
			speedPtr = &speed
		}

		currentMeta[iface] = InterfaceMetadata{
			Interface: iface,
			IPv4:      ipv4,
			IPv6:      ipv6,
			Operstate: operstate,
			Carrier:   carrier,
			MTUBytes:  mtu,
			SpeedMbps: speedPtr,
		}
		sortedIfaces = append(sortedIfaces, iface)
	}
	sort.Strings(sortedIfaces)

	// 2. Emit Snapshot or Change notifications
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
		// Detect removals (sorted alphabetically)
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

		// Detect additions and modifications
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

	// 3. Collect Traffic metrics for matching interfaces
	for _, iface := range sortedIfaces {
		stats := netDevStats[iface]
		var rxRate, txRate, rxPktRate, txPktRate float64

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
				"rx_bits_per_second":    round(rxRate*8, 2),
				"tx_bits_per_second":    round(txRate*8, 2),
			},
		})
	}

	return events, nil
}

func (c *NetworkCollector) shouldKeepInterface(iface, operstate string) bool {
	// 1. Check include_interfaces whitelist
	if c.config != nil && len(c.config.IncludeInterfaces) > 0 {
		return slices.Contains(c.config.IncludeInterfaces, iface)
	}

	// 2. Check loopback
	includeLoopback := false
	if c.config != nil && c.config.IncludeLoopback != nil {
		includeLoopback = *c.config.IncludeLoopback
	}
	if iface == "lo" && !includeLoopback {
		return false
	}

	// 3. Check down status
	includeDown := false
	if c.config != nil && c.config.IncludeDown != nil {
		includeDown = *c.config.IncludeDown
	}
	if operstate == "down" && !includeDown {
		return false
	}

	// 4. Check virtual interfaces
	includeVirtual := false
	if c.config != nil && c.config.IncludeVirtual != nil {
		includeVirtual = *c.config.IncludeVirtual
	}
	if !includeVirtual && isVirtualInterface(c.sysClassNetDir, iface) {
		return false
	}

	// 5. Check exclude_prefixes
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

func getIPs(ifaceName string) (ipv4, ipv6 *string) {
	iface, err := net.InterfaceByName(ifaceName)
	if err != nil {
		return nil, nil
	}
	addrs, err := iface.Addrs()
	if err != nil {
		return nil, nil
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
	return ipv4, ipv6
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
