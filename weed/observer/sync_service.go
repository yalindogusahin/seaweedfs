package observer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

// ObserverSyncService handles asynchronous replication from leader to observer
type ObserverSyncService struct {
	mu sync.RWMutex

	// Configuration
	LeaderAddr   string
	ObserverAddr string
	VolumeId     uint32

	// State
	Status        ObserverSyncStatus
	LastSeq       uint64
	LastSyncTime  time.Time
	LastError     error
	ReconnectCount int

	// Context for cancellation
	ctx    context.Context
	cancel context.CancelFunc

	// Sync interval for polling
	syncInterval time.Duration
}

// NewObserverSyncService creates a new sync service for an observer
func NewObserverSyncService(leaderAddr, observerAddr string, volumeId uint32) *ObserverSyncService {
	ctx, cancel := context.WithCancel(context.Background())
	return &ObserverSyncService{
		LeaderAddr:   leaderAddr,
		ObserverAddr: observerAddr,
		VolumeId:     volumeId,
		Status:       ObserverSyncIdle,
		syncInterval: 1 * time.Second,
		ctx:          ctx,
		cancel:       cancel,
	}
}

// Start begins the sync process
func (s *ObserverSyncService) Start() {
	go s.syncLoop()
	glog.V(1).Infof("Observer sync service started: volume=%d, leader=%s, observer=%s",
		s.VolumeId, s.LeaderAddr, s.ObserverAddr)
}

// Stop stops the sync process
func (s *ObserverSyncService) Stop() {
	s.cancel()
	glog.V(1).Infof("Observer sync service stopped: volume=%d, observer=%s",
		s.VolumeId, s.ObserverAddr)
}

// syncLoop is the main sync loop
func (s *ObserverSyncService) syncLoop() {
	ticker := time.NewTicker(s.syncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			s.setStatus(ObserverSyncIdle)
			return
		case <-ticker.C:
			s.syncOnce()
		}
	}
}

// syncOnce performs a single sync iteration
func (s *ObserverSyncService) syncOnce() {
	s.mu.Lock()
	s.Status = ObserverSyncConnecting
	s.mu.Unlock()

	// Connect to leader and fetch updates
	err := s.fetchAndApplyUpdates()

	s.mu.Lock()
	if err != nil {
		s.LastError = err
		s.Status = ObserverSyncError
		s.ReconnectCount++
		glog.V(2).Infof("Observer sync error: volume=%d, observer=%s, error=%v",
			s.VolumeId, s.ObserverAddr, err)
	} else {
		s.Status = ObserverSyncCaughtUp
		s.LastSyncTime = time.Now()
		s.ReconnectCount = 0
	}
	s.mu.Unlock()
}

// fetchAndApplyUpdates fetches new data from the leader and applies it to the observer
func (s *ObserverSyncService) fetchAndApplyUpdates() error {
	// Connect to the leader volume server
	grpcDialOption := pb.GetGrpcDialOption(false) // Use appropriate security settings

	err := pb.WithVolumeServerClient(false, pb.NewServerAddressFromString(s.LeaderAddr), grpcDialOption,
		func(client volume_server_pb.VolumeServerClient) error {

			// Request incremental sync from last known sequence
			resp, err := client.SyncObserverShard(s.ctx, &volume_server_pb.SyncObserverShardRequest{
				VolumeId:     s.VolumeId,
				LastSequence: s.LastSeq,
				ObserverId:   s.ObserverAddr,
			})

			if err != nil {
				return fmt.Errorf("sync request failed: %w", err)
			}

			if resp.Error != "" {
				return fmt.Errorf("leader returned error: %s", resp.Error)
			}

			if len(resp.Updates) == 0 {
				// No new updates, we're caught up
				return nil
			}

			// Apply updates to observer
			s.mu.Lock()
			s.Status = ObserverSyncSyncing
			s.mu.Unlock()

			// Send updates to observer
			applyErr := s.applyUpdatesToObserver(resp.Updates, resp.LastSequence)
			if applyErr != nil {
				return fmt.Errorf("failed to apply updates: %w", applyErr)
			}

			return nil
		})

	return err
}

// applyUpdatesToObserver sends the updates to the observer volume server
func (s *ObserverSyncService) applyUpdatesToObserver(updates []*volume_server_pb.ShardUpdate, lastSeq uint64) error {
	grpcDialOption := pb.GetGrpcDialOption(false)

	err := pb.WithVolumeServerClient(false, pb.NewServerAddressFromString(s.ObserverAddr), grpcDialOption,
		func(client volume_server_pb.VolumeServerClient) error {

			// Apply updates in batch
			applyResp, err := client.ApplyShardUpdates(s.ctx, &volume_server_pb.ApplyShardUpdatesRequest{
				VolumeId: s.VolumeId,
				Updates:  updates,
			})

			if err != nil {
				return fmt.Errorf("apply updates failed: %w", err)
			}

			if applyResp.Error != "" {
				return fmt.Errorf("observer returned error: %s", applyResp.Error)
			}

			// Update last known sequence
			s.LastSeq = lastSeq

			return nil
		})

	return err
}

// GetStatus returns the current sync status
func (s *ObserverSyncService) GetStatus() ObserverSyncStatus {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.Status
}

// GetLastSyncTime returns the last successful sync time
func (s *ObserverSyncService) GetLastSyncTime() time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.LastSyncTime
}

// GetLagSeconds returns the current lag in seconds
func (s *ObserverSyncService) GetLagSeconds() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.LastSyncTime.IsZero() {
		return 0
	}
	return int64(time.Since(s.LastSyncTime) / time.Second)
}

// IsHealthy checks if the observer is within acceptable lag
func (s *ObserverSyncService) IsHealthy(maxLagSeconds int64) bool {
	return s.GetLagSeconds() <= maxLagSeconds
}

// SyncConfig holds configuration for observer sync
type SyncConfig struct {
	// SyncInterval is how often to check for updates
	SyncInterval time.Duration

	// MaxBatchSize is the maximum number of updates to fetch in one batch
	MaxBatchSize int

	// RetryInterval is how long to wait before retrying after an error
	RetryInterval time.Duration

	// MaxRetries is the maximum number of retries before giving up
	MaxRetries int
}

// DefaultSyncConfig returns default sync configuration
func DefaultSyncConfig() SyncConfig {
	return SyncConfig{
		SyncInterval:  1 * time.Second,
		MaxBatchSize:  1000,
		RetryInterval: 5 * time.Second,
		MaxRetries:    10,
	}
}

// SyncPool manages a pool of sync services
type SyncPool struct {
	mu       sync.RWMutex
	services map[observerKey]*ObserverSyncService
	config   SyncConfig
}

// observerKey uniquely identifies an observer sync
type observerKey struct {
	VolumeId   uint32
	Observer   string
	Leader     string
}

// NewSyncPool creates a new sync pool
func NewSyncPool(config SyncConfig) *SyncPool {
	return &SyncPool{
		services: make(map[observerKey]*ObserverSyncService),
		config:   config,
	}
}

// AddService adds a new sync service to the pool
func (p *SyncPool) AddService(leaderAddr, observerAddr string, volumeId uint32) *ObserverSyncService {
	key := observerKey{
		VolumeId: volumeId,
		Observer: observerAddr,
		Leader:   leaderAddr,
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	// Check if already exists
	if existing, ok := p.services[key]; ok {
		return existing
	}

	// Create new service
	service := NewObserverSyncService(leaderAddr, observerAddr, volumeId)
	service.syncInterval = p.config.SyncInterval
	p.services[key] = service

	// Start the service
	service.Start()

	return service
}

// RemoveService removes and stops a sync service
func (p *SyncPool) RemoveService(volumeId uint32, observerAddr string) {
	key := observerKey{
		VolumeId: volumeId,
		Observer: observerAddr,
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if service, ok := p.services[key]; ok {
		service.Stop()
		delete(p.services, key)
	}
}

// GetService returns a sync service if it exists
func (p *SyncPool) GetService(volumeId uint32, observerAddr string) (*ObserverSyncService, bool) {
	key := observerKey{
		VolumeId: volumeId,
		Observer: observerAddr,
	}

	p.mu.RLock()
	defer p.mu.RUnlock()

	service, ok := p.services[key]
	return service, ok
}

// GetServicesForVolume returns all sync services for a volume
func (p *SyncPool) GetServicesForVolume(volumeId uint32) []*ObserverSyncService {
	p.mu.RLock()
	defer p.mu.RUnlock()

	var services []*ObserverSyncService
	for key, service := range p.services {
		if key.VolumeId == volumeId {
			services = append(services, service)
		}
	}
	return services
}

// StopAll stops all sync services
func (p *SyncPool) StopAll() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for key, service := range p.services {
		service.Stop()
		delete(p.services, key)
	}
}
