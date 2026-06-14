package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// Metrics tracking DNS resolution failures
var dnsResolveFailuresTotal uint64

// Target represents a scrape target
type Target struct {
	ID             string
	OriginalAddr   string
	ResolvedIPs    []string
	LastResolveErr error
	ConsecutiveErr int
	NextResolveAt  time.Time
	ScrapeInterval time.Duration
}

// TargetManager manages the lifecycle of scrape targets
type TargetManager struct {
	mu       sync.RWMutex
	targets  map[string]*Target
	resolver *net.Resolver
}

// NewTargetManager creates a new TargetManager
func NewTargetManager() *TargetManager {
	return &TargetManager{
		targets:  make(map[string]*Target),
		resolver: net.DefaultResolver,
	}
}

// AddTarget adds a new target to the manager
func (tm *TargetManager) AddTarget(id, addr string, scrapeInterval time.Duration) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.targets[id] = &Target{
		ID:             id,
		OriginalAddr:   addr,
		ScrapeInterval: scrapeInterval,
		NextResolveAt:  time.Now(),
	}
}

// ResolveTargets attempts to resolve hostnames for all targets
func (tm *TargetManager) ResolveTargets(ctx context.Context) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	now := time.Now()
	for _, t := range tm.targets {
		if now.Before(t.NextResolveAt) {
			continue
		}

		host, port, err := net.SplitHostPort(t.OriginalAddr)
		if err != nil {
			host = t.OriginalAddr
		}

		// Check if host is already an IP address
		if ip := net.ParseIP(host); ip != nil {
			t.ResolvedIPs = []string{t.OriginalAddr}
			t.LastResolveErr = nil
			t.ConsecutiveErr = 0
			continue
		}

		// Perform DNS resolution
		ips, err := tm.resolver.LookupHost(ctx, host)
		if err != nil {
			atomic.AddUint64(&dnsResolveFailuresTotal, 1)
			t.ConsecutiveErr++
			t.LastResolveErr = err
			t.ResolvedIPs = nil

			// Exponential backoff capped at 2 minutes or scrape interval
			backoffSec := math.Pow(2, float64(t.ConsecutiveErr))
			backoff := time.Duration(backoffSec) * time.Second
			maxBackoff := 2 * time.Minute
			if t.ScrapeInterval > 0 && t.ScrapeInterval < maxBackoff {
				maxBackoff = t.ScrapeInterval
			}
			if backoff > maxBackoff {
				backoff = maxBackoff
			}

			t.NextResolveAt = now.Add(backoff)
			log.Printf("WARN: DNS resolution failed for target %s (%s): %v. Retrying in %v", t.ID, t.OriginalAddr, err, backoff)
		} else {
			if t.ConsecutiveErr > 0 {
				log.Printf("INFO: DNS resolution recovered for target %s (%s) after %d failures", t.ID, t.OriginalAddr, t.ConsecutiveErr)
			}
			resolved := make([]string, len(ips))
			for i, ip := range ips {
				if port != "" {
					resolved[i] = net.JoinHostPort(ip, port)
				} else {
					resolved[i] = ip
				}
			}
			t.ResolvedIPs = resolved
			t.LastResolveErr = nil
			t.ConsecutiveErr = 0
			t.NextResolveAt = now.Add(t.ScrapeInterval)
		}
	}
}

// GetActiveTargets returns the list of currently resolved targets
func (tm *TargetManager) GetActiveTargets() map[string][]string {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	active := make(map[string][]string)
	for id, t := range tm.targets {
		if len(t.ResolvedIPs) > 0 {
			active[id] = t.ResolvedIPs
		}
	}
	return active
}

func main() {
	log.Println("Starting resilient vmagent target manager simulation...")
	tm := NewTargetManager()

	// Add a mock target that requires DNS resolution
	tm.AddTarget("mock-service", "mock-service.local:8080", 10*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Simulate periodic resolution loop
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
			case <-ticker.C:
				tm.ResolveTargets(ctx)
				active := tm.GetActiveTargets()
				log.Printf("Active scrape targets: %v, DNS failures total: %d", active, atomic.LoadUint64(&dnsResolveFailuresTotal))
			case <-ctx.Done():
				log.Println("Simulation finished.")
				return
		}
	}
}
