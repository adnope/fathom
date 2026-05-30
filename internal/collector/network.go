package collector

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
)

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
	peakRxRate     map[string]float64
	peakTxRate     map[string]float64
	hasSnapshot    bool
}

// NewNetworkCollector constructs a NetworkCollector with default paths.
func NewNetworkCollector() *NetworkCollector {
	return &NetworkCollector{
		procNetDevPath: "/proc/net/dev",
		sysClassNetDir: "/sys/class/net",
		cachedMeta:     make(map[string]InterfaceMetadata),
		prevMetrics:    make(map[string]InterfaceMetrics),
		prevTime:       make(map[string]time.Time),
		peakRxRate:     make(map[string]float64),
		peakTxRate:     make(map[string]float64),
	}
}

// Name returns the name of the collector.
func (c *NetworkCollector) Name() string {
	return "network"
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

	// 1. Gather current metadata
	currentMeta := make(map[string]InterfaceMetadata)
	for iface := range netDevStats {
		ipv4, ipv6 := getIPs(iface)

		operstate, _ := readSysFileString(filepath.Join(c.sysClassNetDir, iface, "operstate"))
		if operstate == "" {
			operstate = "unknown"
		}

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
	}

	// 2. Emit Snapshot or Change notifications
	if !c.hasSnapshot {
		var interfacesList []any
		for _, m := range currentMeta {
			interfacesList = append(interfacesList, m)
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
		// Detect removals
		for iface, oldM := range c.cachedMeta {
			if _, exists := currentMeta[iface]; !exists {
				events = append(events, Event{
					Event:     "network_metadata_changed",
					Collector: "network",
					Component: "collector",
					Data: map[string]any{
						"interface":      iface,
						"old":            oldM,
						"new":            nil,
						"changed_fields": []string{"interface_removed"},
					},
				})
			}
		}

		// Detect additions and changes
		for iface, newM := range currentMeta {
			oldM, exists := c.cachedMeta[iface]
			if !exists {
				events = append(events, Event{
					Event:     "network_metadata_changed",
					Collector: "network",
					Component: "collector",
					Data: map[string]interface{}{
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

	// 3. Collect Traffic metrics
	for iface, stats := range netDevStats {
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

		if rxRate > c.peakRxRate[iface] {
			c.peakRxRate[iface] = rxRate
		}
		if txRate > c.peakTxRate[iface] {
			c.peakTxRate[iface] = txRate
		}

		events = append(events, Event{
			Event:     "metric_sample",
			Collector: "network",
			Data: map[string]any{
				"interface":               iface,
				"rx_bytes_total":          stats.rxBytes,
				"tx_bytes_total":          stats.txBytes,
				"rx_packets_total":        stats.rxPackets,
				"tx_packets_total":        stats.txPackets,
				"rx_errors_total":         stats.rxErrors,
				"tx_errors_total":         stats.txErrors,
				"rx_dropped_total":        stats.rxDropped,
				"tx_dropped_total":        stats.txDropped,
				"rx_bytes_per_second":     rxRate,
				"tx_bytes_per_second":     txRate,
				"rx_packets_per_second":   rxPktRate,
				"tx_packets_per_second":   txPktRate,
				"rx_bits_per_second":      rxRate * 8,
				"tx_bits_per_second":      txRate * 8,
				"rx_top_bytes_per_second": c.peakRxRate[iface],
				"tx_top_bytes_per_second": c.peakTxRate[iface],
			},
		})
	}

	return events, nil
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
		if iface == "lo" || iface == "" {
			continue
		}

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
	return res, scanner.Err()
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
