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

type networkRates struct {
	rxBytesRate           float64
	txBytesRate           float64
	rxPacketsRate         float64
	txPacketsRate         float64
	rxErrorsRate          float64
	txErrorsRate          float64
	rxDroppedRate         float64
	txDroppedRate         float64
	sampleIntervalSeconds float64
	hasSampleInterval     bool
}

// InterfaceMetadata represents semi-dynamic properties of a network interface.
type InterfaceMetadata struct {
	Interface  string  `json:"interface"`
	Type       string  `json:"interface_type"`
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
	issues         *collectorIssueLogger
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
		issues:         newCollectorIssueLogger(),
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
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	netDevStats, err := parseProcNetDev(c.procNetDevPath)
	if err != nil {
		return nil, fmt.Errorf("read required network source %s: %w", c.procNetDevPath, err)
	}

	var events []Event
	now := time.Now()
	currentMeta, sortedIfaces := c.collectNetworkMetadata(netDevStats)

	if !c.hasSnapshot {
		events = append(events, buildNetworkMetadataSnapshotEvent(currentMeta, sortedIfaces))
		c.cachedMeta = currentMeta
		c.hasSnapshot = true
	} else {
		events = append(events, buildNetworkMetadataChangeEvents(c.cachedMeta, currentMeta, sortedIfaces)...)
		c.cachedMeta = currentMeta
	}

	for _, iface := range sortedIfaces {
		stats := netDevStats[iface]
		prev, ok := c.prevMetrics[iface]
		prevT, okT := c.prevTime[iface]
		if ok && networkCounterReset(stats, prev) {
			c.issueLogger().log(slog.LevelDebug, c.Name(), actionZeroMetric, "network_rates", c.procNetDevPath, nil,
				slog.String("resource_type", resourceTypeInterface),
				slog.String("resource", iface),
			)
		} else {
			c.issueLogger().clear(c.Name(), actionZeroMetric, "network_rates", c.procNetDevPath,
				slog.String("resource_type", resourceTypeInterface),
				slog.String("resource", iface),
			)
		}
		rates := calculateNetworkRates(stats, prev, ok, prevT, okT, now)

		c.prevMetrics[iface] = interfaceMetricsFromRaw(stats)
		c.prevTime[iface] = now
		events = append(events, buildNetworkMetricEvent(currentMeta[iface], stats, rates))
	}

	return events, nil
}

func (c *NetworkCollector) issueLogger() *collectorIssueLogger {
	if c.issues == nil {
		c.issues = newCollectorIssueLogger()
	}
	return c.issues
}

func (c *NetworkCollector) collectNetworkMetadata(netDevStats map[string]rawNetDev) (map[string]InterfaceMetadata, []string) {
	currentMeta := make(map[string]InterfaceMetadata)
	var sortedIfaces []string

	for iface := range netDevStats {
		metadata, ok := c.readInterfaceMetadata(iface)
		if !ok {
			continue
		}
		currentMeta[iface] = metadata
		sortedIfaces = append(sortedIfaces, iface)
	}
	sort.Strings(sortedIfaces)

	return currentMeta, sortedIfaces
}

func (c *NetworkCollector) readInterfaceMetadata(iface string) (InterfaceMetadata, bool) {
	operstate, _ := readSysFileString(filepath.Join(c.sysClassNetDir, iface, "operstate"))
	if operstate == "" {
		operstate = "unknown"
	}

	if !c.shouldKeepInterface(iface, operstate) {
		return InterfaceMetadata{}, false
	}

	ifaceType := c.interfaceType(iface)
	ipv4, ipv6 := c.getIPsWithFallbacks(iface)
	carrierPtr := c.readOptionalInterfaceInt(iface, "carrier", filepath.Join(c.sysClassNetDir, iface, "carrier"), false)

	mtu := 1500
	if sysIface, err := net.InterfaceByName(iface); err == nil {
		mtu = sysIface.MTU
	}

	var speedPtr *int
	var duplexPtr *string
	if ifaceType != "wireless" {
		speedPtr = c.readOptionalInterfaceInt(iface, "speed_mbps", filepath.Join(c.sysClassNetDir, iface, "speed"), true)
		duplexPtr = c.readOptionalInterfaceString(iface, "duplex", filepath.Join(c.sysClassNetDir, iface, "duplex"))
	}
	txQueueLenPtr := c.readOptionalInterfaceInt(iface, "tx_queue_len", filepath.Join(c.sysClassNetDir, iface, "tx_queue_len"), false)

	return InterfaceMetadata{
		Interface:  iface,
		Type:       ifaceType,
		IPv4:       ipv4,
		IPv6:       ipv6,
		Operstate:  operstate,
		Carrier:    carrierPtr,
		MTUBytes:   mtu,
		SpeedMbps:  speedPtr,
		TxQueueLen: txQueueLenPtr,
		Duplex:     duplexPtr,
	}, true
}

func (c *NetworkCollector) readOptionalInterfaceInt(iface, metric, path string, requirePositive bool) *int {
	value, err := readSysFileInt(path)
	if err != nil {
		c.logOptionalInterfaceIssue(iface, metric, path, err)
		return nil
	}
	if requirePositive && value <= 0 {
		c.logOptionalInterfaceIssue(iface, metric, path, fmt.Errorf("non-positive value %d", value))
		return nil
	}
	c.clearOptionalInterfaceIssue(iface, metric, path)
	return &value
}

func (c *NetworkCollector) readOptionalInterfaceString(iface, metric, path string) *string {
	value, err := readSysFileString(path)
	if err != nil {
		c.logOptionalInterfaceIssue(iface, metric, path, err)
		return nil
	}
	if value == "" {
		c.logOptionalInterfaceIssue(iface, metric, path, fmt.Errorf("empty value"))
		return nil
	}
	c.clearOptionalInterfaceIssue(iface, metric, path)
	return &value
}

func (c *NetworkCollector) logOptionalInterfaceIssue(iface, metric, path string, err error) {
	level := slog.LevelDebug
	if c.isConfiguredInterface(iface) {
		level = slog.LevelWarn
	}
	c.issueLogger().log(level, c.Name(), actionOmitMetric, metric, path, err,
		slog.String("resource_type", resourceTypeInterface),
		slog.String("resource", iface),
	)
}

func (c *NetworkCollector) clearOptionalInterfaceIssue(iface, metric, path string) {
	c.issueLogger().clear(c.Name(), actionOmitMetric, metric, path,
		slog.String("resource_type", resourceTypeInterface),
		slog.String("resource", iface),
	)
}

func buildNetworkMetadataSnapshotEvent(currentMeta map[string]InterfaceMetadata, sortedIfaces []string) Event {
	var interfacesList []any
	for _, iface := range sortedIfaces {
		interfacesList = append(interfacesList, currentMeta[iface])
	}

	return Event{
		Event:     "network_metadata_snapshot",
		Collector: "network",
		Component: componentCollector,
		Data: map[string]any{
			"interfaces": interfacesList,
		},
	}
}

func buildNetworkMetadataChangeEvents(oldMeta, currentMeta map[string]InterfaceMetadata, sortedIfaces []string) []Event {
	var events []Event
	var removedKeys []string
	for iface := range oldMeta {
		if _, exists := currentMeta[iface]; !exists {
			removedKeys = append(removedKeys, iface)
		}
	}
	sort.Strings(removedKeys)

	for _, iface := range removedKeys {
		events = append(events, Event{
			Event:     "network_metadata_changed",
			Collector: "network",
			Component: componentCollector,
			Data: map[string]any{
				"interface":      iface,
				"old":            oldMeta[iface],
				"new":            nil,
				"changed_fields": []string{"interface_removed"},
			},
		})
	}

	for _, iface := range sortedIfaces {
		newMetadata := currentMeta[iface]
		oldMetadata, exists := oldMeta[iface]
		if !exists {
			events = append(events, Event{
				Event:     "network_metadata_changed",
				Collector: "network",
				Component: componentCollector,
				Data: map[string]any{
					"interface":      iface,
					"old":            nil,
					"new":            newMetadata,
					"changed_fields": []string{"interface_added"},
				},
			})
			continue
		}

		if reflect.DeepEqual(oldMetadata, newMetadata) {
			continue
		}

		changed := changedNetworkMetadataFields(oldMetadata, newMetadata)
		if len(changed) == 0 {
			continue
		}

		events = append(events, Event{
			Event:     "network_metadata_changed",
			Collector: "network",
			Component: componentCollector,
			Data: map[string]any{
				"interface":      iface,
				"old":            oldMetadata,
				"new":            newMetadata,
				"changed_fields": changed,
			},
		})
	}

	return events
}

func changedNetworkMetadataFields(oldMetadata, newMetadata InterfaceMetadata) []string {
	var changed []string
	if !equalStringPtr(oldMetadata.IPv4, newMetadata.IPv4) {
		changed = append(changed, "ipv4")
	}
	if !equalStringPtr(oldMetadata.IPv6, newMetadata.IPv6) {
		changed = append(changed, "ipv6")
	}
	if oldMetadata.Type != newMetadata.Type {
		changed = append(changed, "interface_type")
	}
	if oldMetadata.Operstate != newMetadata.Operstate {
		changed = append(changed, "operstate")
	}
	if oldMetadata.Carrier != newMetadata.Carrier {
		changed = append(changed, "carrier")
	}
	if oldMetadata.MTUBytes != newMetadata.MTUBytes {
		changed = append(changed, "mtu_bytes")
	}
	if !equalIntPtr(oldMetadata.SpeedMbps, newMetadata.SpeedMbps) {
		changed = append(changed, "speed_mbps")
	}
	if !equalIntPtr(oldMetadata.TxQueueLen, newMetadata.TxQueueLen) {
		changed = append(changed, "tx_queue_len")
	}
	if !equalStringPtr(oldMetadata.Duplex, newMetadata.Duplex) {
		changed = append(changed, "duplex")
	}
	return changed
}

func calculateNetworkRates(stats rawNetDev, prev InterfaceMetrics, hasPrev bool, prevTime time.Time, hasPrevTime bool, now time.Time) networkRates {
	if !hasPrev || !hasPrevTime {
		return networkRates{}
	}

	duration := now.Sub(prevTime).Seconds()
	if duration <= 0 {
		return networkRates{}
	}

	rates := networkRates{
		sampleIntervalSeconds: duration,
		hasSampleInterval:     true,
	}
	if diff, ok := counterDiff(stats.rxBytes, prev.rxBytes); ok {
		rates.rxBytesRate = float64(diff) / duration
	}
	if diff, ok := counterDiff(stats.txBytes, prev.txBytes); ok {
		rates.txBytesRate = float64(diff) / duration
	}
	if diff, ok := counterDiff(stats.rxPackets, prev.rxPackets); ok {
		rates.rxPacketsRate = float64(diff) / duration
	}
	if diff, ok := counterDiff(stats.txPackets, prev.txPackets); ok {
		rates.txPacketsRate = float64(diff) / duration
	}
	if diff, ok := counterDiff(stats.rxErrors, prev.rxErrors); ok {
		rates.rxErrorsRate = float64(diff) / duration
	}
	if diff, ok := counterDiff(stats.txErrors, prev.txErrors); ok {
		rates.txErrorsRate = float64(diff) / duration
	}
	if diff, ok := counterDiff(stats.rxDropped, prev.rxDropped); ok {
		rates.rxDroppedRate = float64(diff) / duration
	}
	if diff, ok := counterDiff(stats.txDropped, prev.txDropped); ok {
		rates.txDroppedRate = float64(diff) / duration
	}

	return rates
}

func interfaceMetricsFromRaw(stats rawNetDev) InterfaceMetrics {
	return InterfaceMetrics{
		rxBytes:   stats.rxBytes,
		txBytes:   stats.txBytes,
		rxPackets: stats.rxPackets,
		txPackets: stats.txPackets,
		rxErrors:  stats.rxErrors,
		txErrors:  stats.txErrors,
		rxDropped: stats.rxDropped,
		txDropped: stats.txDropped,
	}
}

func networkCounterReset(stats rawNetDev, prev InterfaceMetrics) bool {
	return stats.rxBytes < prev.rxBytes ||
		stats.txBytes < prev.txBytes ||
		stats.rxPackets < prev.rxPackets ||
		stats.txPackets < prev.txPackets ||
		stats.rxErrors < prev.rxErrors ||
		stats.txErrors < prev.txErrors ||
		stats.rxDropped < prev.rxDropped ||
		stats.txDropped < prev.txDropped
}

func buildNetworkMetricEvent(metadata InterfaceMetadata, stats rawNetDev, rates networkRates) Event {
	iface := metadata.Interface
	var carrier any
	if metadata.Carrier != nil {
		carrier = *metadata.Carrier
	}

	return Event{
		Event:     eventMetricSample,
		Collector: "network",
		Component: componentCollector,
		Data: map[string]any{
			"resource_type":           resourceTypeInterface,
			"resource_id":             resourceID(resourceTypeInterface, iface),
			"interface":               iface,
			"interface_type":          metadata.Type,
			"operstate":               metadata.Operstate,
			"carrier":                 carrier,
			"sample_interval_seconds": optionalRoundedFloat(rates.hasSampleInterval, rates.sampleIntervalSeconds, 3),
			"rx_bytes_total":          stats.rxBytes,
			"tx_bytes_total":          stats.txBytes,
			"rx_packets_total":        stats.rxPackets,
			"tx_packets_total":        stats.txPackets,
			"rx_errors_total":         stats.rxErrors,
			"tx_errors_total":         stats.txErrors,
			"rx_dropped_total":        stats.rxDropped,
			"tx_dropped_total":        stats.txDropped,
			"rx_bytes_per_second":     optionalRoundedFloat(rates.hasSampleInterval, rates.rxBytesRate, 2),
			"tx_bytes_per_second":     optionalRoundedFloat(rates.hasSampleInterval, rates.txBytesRate, 2),
			"rx_packets_per_second":   optionalRoundedFloat(rates.hasSampleInterval, rates.rxPacketsRate, 2),
			"tx_packets_per_second":   optionalRoundedFloat(rates.hasSampleInterval, rates.txPacketsRate, 2),
			"rx_errors_per_second":    optionalRoundedFloat(rates.hasSampleInterval, rates.rxErrorsRate, 2),
			"tx_errors_per_second":    optionalRoundedFloat(rates.hasSampleInterval, rates.txErrorsRate, 2),
			"rx_dropped_per_second":   optionalRoundedFloat(rates.hasSampleInterval, rates.rxDroppedRate, 2),
			"tx_dropped_per_second":   optionalRoundedFloat(rates.hasSampleInterval, rates.txDroppedRate, 2),
		},
	}
}

func (c *NetworkCollector) interfaceType(iface string) string {
	if iface == "lo" {
		return "loopback"
	}
	if _, err := os.Stat(filepath.Join(c.sysClassNetDir, iface, "wireless")); err == nil {
		return "wireless"
	}
	if isVirtualInterface(c.sysClassNetDir, iface) {
		return "virtual"
	}
	return "ethernet"
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
	sawInterfaceLine := false
	for scanner.Scan() {
		lineCount++
		if lineCount <= 2 {
			continue
		}
		if strings.Contains(scanner.Text(), ":") {
			sawInterfaceLine = true
		}
		stat, ok := parseProcNetDevLine(scanner.Text())
		if !ok {
			continue
		}
		res[stat.iface] = stat
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if sawInterfaceLine && len(res) == 0 {
		return nil, fmt.Errorf("no usable network interface lines")
	}
	return res, nil
}

func parseProcNetDevLine(line string) (rawNetDev, bool) {
	parts := strings.SplitN(line, ":", 2)
	if len(parts) < 2 {
		return rawNetDev{}, false
	}
	iface := strings.TrimSpace(parts[0])

	fields := strings.Fields(parts[1])
	if len(fields) < 16 {
		return rawNetDev{}, false
	}

	rxBytes, err := strconv.ParseUint(fields[0], 10, 64)
	if err != nil {
		return rawNetDev{}, false
	}
	rxPackets, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		return rawNetDev{}, false
	}
	rxErrors, err := strconv.ParseUint(fields[2], 10, 64)
	if err != nil {
		return rawNetDev{}, false
	}
	rxDropped, err := strconv.ParseUint(fields[3], 10, 64)
	if err != nil {
		return rawNetDev{}, false
	}

	txBytes, err := strconv.ParseUint(fields[8], 10, 64)
	if err != nil {
		return rawNetDev{}, false
	}
	txPackets, err := strconv.ParseUint(fields[9], 10, 64)
	if err != nil {
		return rawNetDev{}, false
	}
	txErrors, err := strconv.ParseUint(fields[10], 10, 64)
	if err != nil {
		return rawNetDev{}, false
	}
	txDropped, err := strconv.ParseUint(fields[11], 10, 64)
	if err != nil {
		return rawNetDev{}, false
	}

	return rawNetDev{
		iface:     iface,
		rxBytes:   rxBytes,
		rxPackets: rxPackets,
		rxErrors:  rxErrors,
		rxDropped: rxDropped,
		txBytes:   txBytes,
		txPackets: txPackets,
		txErrors:  txErrors,
		txDropped: txDropped,
	}, true
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
		c.clearIPIssue(ifaceName)
		return ip4, ip6
	}
	errs = append(errs, fmt.Sprintf("net.InterfaceByName: %v", err))

	// Method 2: standard net.Interfaces list search
	ip4, ip6, err = getIPsFallbackList(ifaceName)
	if err == nil {
		c.clearIPIssue(ifaceName)
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
		c.clearIPIssue(ifaceName)
		return fallbackIPv4, fallbackIPv6
	}

	level := slog.LevelDebug
	if c.isConfiguredInterface(ifaceName) {
		level = slog.LevelWarn
	}
	c.issueLogger().log(level, c.Name(), actionOmitMetric, "ip_address", ifaceName, fmt.Errorf("%s", strings.Join(errs, "; ")),
		slog.String("resource_type", resourceTypeInterface),
		slog.String("resource", ifaceName),
	)

	return nil, nil
}

func (c *NetworkCollector) clearIPIssue(ifaceName string) {
	c.issueLogger().clear(c.Name(), actionOmitMetric, "ip_address", ifaceName,
		slog.String("resource_type", resourceTypeInterface),
		slog.String("resource", ifaceName),
	)
}

func (c *NetworkCollector) isConfiguredInterface(ifaceName string) bool {
	return c.config != nil && slices.Contains(c.config.IncludeInterfaces, ifaceName)
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
