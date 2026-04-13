package observer

import (
	"context"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

// Manager handles observer lifecycle and monitoring
type Manager struct {
	config ObserverConfig

	// observerStatus maps volume ID to observer server address to status
	observerStatus map[needle.VolumeId]map[string]*ObserverStatus

	// syncServices tracks active sync services for each observer
	syncServices map[needle.VolumeId]map[string]*ObserverSyncService

	// lock for thread safety
	mu sync.RWMutex

	// health check interval
	healthCheckInterval time.Duration

	// shutdown channel
	shutdown chan struct{}
}

// NewManager creates a new observer manager
func NewManager(config ObserverConfig) *Manager {
	return &Manager{
		config:              config,
		observerStatus:      make(map[needle.VolumeId]map[string]*ObserverStatus),
		syncServices:        make(map[needle.VolumeId]map[string]*ObserverSyncService),
		healthCheckInterval: 10 * time.Second,
		shutdown:            make(chan struct{}),
	}
}

// Start begins the observer health check loop
func (m *Manager) Start(ctx context.Context) {
	go m.healthCheckLoop(ctx)
	glog.V(0).Infof("Observer manager started with config: %+v", m.config)
}

// Stop stops the observer manager
func (m *Manager) Stop() {
	close(m.shutdown)
	glog.V(0).Info("Observer manager stopped")
}

// healthCheckLoop periodically checks observer health
func (m *Manager) healthCheckLoop(ctx context.Context) {
	ticker := time.NewTicker(m.healthCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-m.shutdown:
			return
		case <-ticker.C:
			m.checkObserverHealth()
		}
	}
}

// checkObserverHealth checks all observers and updates their health status
func (m *Manager) checkObserverHealth() {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for vid, observers := range m.observerStatus {
		for serverAddr, status := range observers {
			// Calculate lag
			lag := time.Now().UnixNano() - status.LastSyncNs
			lagSeconds := lag / 1e9
			status.LagSeconds = lagSeconds

			// Update health status
			status.IsHealthy = lagSeconds <= m.config.MaxLagSeconds

			if !status.IsHealthy && status.LastError == "" {
				status.LastError = "lag exceeds maximum allowed"
			}

			// Check for auto-promotion
			if m.config.AutoPromotion.Enabled && !status.IsHealthy {
				m.checkAutoPromotion(vid, serverAddr, status)
			}
		}
	}
}

// checkAutoPromotion checks if an observer should be promoted to sync replica
func (m *Manager) checkAutoPromotion(vid needle.VolumeId, observerAddr string, status *ObserverStatus) {
	// Get current sync replica count (would need topology access)
	// For now, just log the condition
	glog.V(2).Infof("Observer %s for volume %d is unhealthy (lag: %ds), consider promotion",
		observerAddr, vid, status.LagSeconds)
}

// RegisterObserver registers an observer for a volume
func (m *Manager) RegisterObserver(vid needle.VolumeId, observerAddr, leaderAddr string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.observerStatus[vid]; !exists {
		m.observerStatus[vid] = make(map[string]*ObserverStatus)
	}

	if _, exists := m.syncServices[vid]; !exists {
		m.syncServices[vid] = make(map[string]*ObserverSyncService)
	}

	// Check if observer already registered
	if _, exists := m.observerStatus[vid][observerAddr]; exists {
		glog.V(2).Infof("Observer %s already registered for volume %d", observerAddr, vid)
		return nil
	}

	// Create initial status
	m.observerStatus[vid][observerAddr] = &ObserverStatus{
		VolumeId:     vid,
		ObserverServer: observerAddr,
		LeaderServer:   leaderAddr,
		LastSyncNs:     time.Now().UnixNano(),
		LagSeconds:     0,
		IsHealthy:      true,
	}

	// Create sync service
	m.syncServices[vid][observerAddr] = &ObserverSyncService{
		LeaderAddr:   leaderAddr,
		ObserverAddr: observerAddr,
		VolumeId:     vid,
		Status:       ObserverSyncIdle,
	}

	glog.V(1).Infof("Registered observer %s for volume %d (leader: %s)",
		observerAddr, vid, leaderAddr)

	return nil
}

// UnregisterObserver removes an observer from a volume
func (m *Manager) UnregisterObserver(vid needle.VolumeId, observerAddr string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if observers, exists := m.observerStatus[vid]; exists {
		delete(observers, observerAddr)
		if len(observers) == 0 {
			delete(m.observerStatus, vid)
		}
	}

	if services, exists := m.syncServices[vid]; exists {
		delete(services, observerAddr)
		if len(services) == 0 {
			delete(m.syncServices, vid)
		}
	}

	glog.V(1).Infof("Unregistered observer %s for volume %d", observerAddr, vid)
	return nil
}

// GetObserverStatus returns the status of an observer
func (m *Manager) GetObserverStatus(vid needle.VolumeId, observerAddr string) (*ObserverStatus, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if observers, exists := m.observerStatus[vid]; exists {
		if status, exists := observers[observerAddr]; exists {
			return status, nil
		}
	}

	return nil, &ObserverError{Message: "observer not found"}
}

// GetObserverStatuses returns all observer statuses for a volume
func (m *Manager) GetObserverStatuses(vid needle.VolumeId) map[string]*ObserverStatus {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if observers, exists := m.observerStatus[vid]; exists {
		// Return a copy to avoid external modification
		statuses := make(map[string]*ObserverStatus, len(observers))
		for k, v := range observers {
			statuses[k] = v
		}
		return statuses
	}

	return nil
}

// GetHealthyObservers returns healthy observers for a volume
func (m *Manager) GetHealthyObservers(vid needle.VolumeId) []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var healthy []string
	if observers, exists := m.observerStatus[vid]; exists {
		for addr, status := range observers {
			if status.IsHealthy {
				healthy = append(healthy, addr)
			}
		}
	}

	return healthy
}

// PromoteObserver promotes an observer to a sync replica
func (m *Manager) PromoteObserver(vid needle.VolumeId, observerAddr string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	status, exists := m.observerStatus[vid][observerAddr]
	if !exists {
		return &ObserverError{Message: "observer not found"}
	}

	// Check if observer is healthy enough to promote
	if status.LagSeconds > m.config.AutoPromotion.PromoteOnLagSeconds {
		return &ObserverError{Message: "observer lag too large for safe promotion"}
	}

	// Mark for promotion (actual promotion handled by topology/volume layout)
	status.IsHealthy = true
	glog.V(0).Infof("Promoting observer %s for volume %d to sync replica", observerAddr, vid)

	return nil
}

// AssignObservers assigns observer replicas for a volume based on placement config
func (m *Manager) AssignObservers(
	vid needle.VolumeId,
	leaderAddr pb.ServerAddress,
	syncReplicas []pb.ServerAddress,
	observerCount int,
	availableServers []pb.ServerAddress,
) ([]pb.ServerAddress, error) {

	if observerCount <= 0 {
		return nil, nil
	}

	// Build set of already assigned servers (leader + sync replicas)
	assigned := make(map[string]bool)
	assigned[leaderAddr.String()] = true
	for _, replica := range syncReplicas {
		assigned[replica.String()] = true
	}

	// Select observers from available servers, avoiding duplicates
	var observers []pb.ServerAddress
	for _, server := range availableServers {
		if len(observers) >= observerCount {
			break
		}
		if !assigned[server.String()] {
			assigned[server.String()] = true
			observers = append(observers, server)

			// Register the observer
			if err := m.RegisterObserver(vid, server.String(), leaderAddr.String()); err != nil {
				glog.V(2).Infof("Failed to register observer %s: %v", server.String(), err)
			}
		}
	}

	if len(observers) < observerCount {
		glog.V(2).Infof("Could only assign %d observers for volume %d, requested %d",
			len(observers), vid, observerCount)
	}

	return observers, nil
}
