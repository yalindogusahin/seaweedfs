package observer

import (
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
)

// ReplicaRole defines the role of a volume replica in the cluster
type ReplicaRole int

const (
	// ReplicaRoleLeader is the primary replica that handles all writes
	ReplicaRoleLeader ReplicaRole = iota

	// ReplicaRoleSyncFollower is a synchronous replica that must acknowledge writes
	ReplicaRoleSyncFollower

	// ReplicaRoleObserver is an asynchronous replica that does not block writes
	ReplicaRoleObserver
)

func (r ReplicaRole) String() string {
	switch r {
	case ReplicaRoleLeader:
		return "leader"
	case ReplicaRoleSyncFollower:
		return "sync_follower"
	case ReplicaRoleObserver:
		return "observer"
	default:
		return "unknown"
	}
}

// ParseReplicaRole parses a string into a ReplicaRole
func ParseReplicaRole(s string) (ReplicaRole, error) {
	switch s {
	case "leader", "0":
		return ReplicaRoleLeader, nil
	case "sync_follower", "follower", "1":
		return ReplicaRoleSyncFollower, nil
	case "observer", "2":
		return ReplicaRoleObserver, nil
	default:
		return ReplicaRoleLeader, ErrInvalidReplicaRole
	}
}

// ObserverConfig holds configuration for observer functionality
type ObserverConfig struct {
	// Enabled determines if observer support is active
	Enabled bool `json:"enabled"`

	// MaxLagSeconds is the maximum allowed lag for an observer before it's considered unhealthy
	MaxLagSeconds int64 `json:"max_lag_seconds"`

	// MinObservers is the minimum number of healthy observers required
	MinObservers int `json:"min_observers"`

	// AutoPromotion configuration
	AutoPromotion ObserverAutoPromotion `json:"auto_promotion"`
}

// DefaultObserverConfig returns a sensible default configuration
func DefaultObserverConfig() ObserverConfig {
	return ObserverConfig{
		Enabled:       true,
		MaxLagSeconds: 300, // 5 minutes
		MinObservers:  0,
		AutoPromotion: DefaultAutoPromotion(),
	}
}

// ObserverAutoPromotion configures automatic observer promotion behavior
type ObserverAutoPromotion struct {
	// Enabled allows automatic promotion of observers to sync replicas
	Enabled bool `json:"enabled"`

	// MinSyncReplicas is the minimum number of sync replicas required
	// If fewer sync replicas are available, observers may be promoted
	MinSyncReplicas int `json:"min_sync_replicas"`

	// PromoteOnLagSeconds triggers promotion when sync replicas lag too far behind
	PromoteOnLagSeconds int64 `json:"promote_on_lag_seconds"`
}

// DefaultAutoPromotion returns default auto-promotion settings
func DefaultAutoPromotion() ObserverAutoPromotion {
	return ObserverAutoPromotion{
		Enabled:           true,
		MinSyncReplicas:   1,
		PromoteOnLagSeconds: 60, // 1 minute
	}
}

// ExtendedReplicaPlacement extends the basic ReplicaPlacement with observer support
type ExtendedReplicaPlacement struct {
	*super_block.ReplicaPlacement

	// ObserverCount is the number of observer replicas for this volume
	ObserverCount int `json:"observer_count"`
}

// NewExtendedReplicaPlacement creates an extended replica placement from a string
// Format: "DRS" or "DRS+O" where D=DC, R=Rack, S=SameRack, O=Observer count
// Examples: "001", "100+1", "002+2"
func NewExtendedReplicaPlacement(s string) (*ExtendedReplicaPlacement, error) {
	erp := &ExtendedReplicaPlacement{}

	// Check for observer suffix
	erp.ReplicaPlacement = &super_block.ReplicaPlacement{}
	if len(s) == 0 {
		s = "000"
	}

	// Parse base replica placement (first 3 digits)
	base := s
	observerCount := 0

	if idx := findPlusSign(s); idx > 0 && idx < len(s)-1 {
		base = s[:idx]
		observerStr := s[idx+1:]
		// Parse observer count
		for _, c := range observerStr {
			if c >= '0' && c <= '9' {
				observerCount = observerCount*10 + int(c-'0')
			}
		}
	}

	// Parse base replica placement
	var err error
	erp.ReplicaPlacement, err = super_block.NewReplicaPlacementFromString(base)
	if err != nil {
		return nil, err
	}

	erp.ObserverCount = observerCount
	return erp, nil
}

func findPlusSign(s string) int {
	for i, c := range s {
		if c == '+' {
			return i
		}
	}
	return -1
}

// GetTotalReplicaCount returns total number of replicas (sync + observers)
func (erp *ExtendedReplicaPlacement) GetTotalReplicaCount() int {
	return erp.ReplicaPlacement.GetCopyCount() + erp.ObserverCount
}

// GetSyncReplicaCount returns the number of synchronous replicas
func (erp *ExtendedReplicaPlacement) GetSyncReplicaCount() int {
	return erp.ReplicaPlacement.GetCopyCount()
}

// ObserverStatus tracks the status of an observer replica
type ObserverStatus struct {
	// VolumeId is the volume being observed
	VolumeId uint32 `json:"volume_id"`

	// ObserverServer is the address of the observer server
	ObserverServer string `json:"observer_server"`

	// LeaderServer is the address of the leader/primary server
	LeaderServer string `json:"leader_server"`

	// LastSyncNs is the last successful sync timestamp (nanoseconds)
	LastSyncNs int64 `json:"last_sync_ns"`

	// LagSeconds is the current lag behind the leader
	LagSeconds int64 `json:"lag_seconds"`

	// IsHealthy indicates if the observer is within acceptable lag
	IsHealthy bool `json:"is_healthy"`

	// LastError is the last error encountered during sync (if any)
	LastError string `json:"last_error,omitempty"`
}

// IsStale checks if the observer is stale (lagging too much)
func (os *ObserverStatus) IsStale(maxLagSeconds int64) bool {
	return os.LagSeconds > maxLagSeconds
}

// ObserverSyncStatus represents the current sync state
type ObserverSyncStatus int

const (
	ObserverSyncIdle ObserverSyncStatus = iota
	ObserverSyncConnecting
	ObserverSyncSyncing
	ObserverSyncCaughtUp
	ObserverSyncError
)

func (s ObserverSyncStatus) String() string {
	switch s {
	case ObserverSyncIdle:
		return "idle"
	case ObserverSyncConnecting:
		return "connecting"
	case ObserverSyncSyncing:
		return "syncing"
	case ObserverSyncCaughtUp:
		return "caught_up"
	case ObserverSyncError:
		return "error"
	default:
		return "unknown"
	}
}

// Errors for observer operations
var (
	ErrInvalidReplicaRole     = &ObserverError{"invalid replica role"}
	ErrObserverNotConfigured  = &ObserverError{"observer not configured"}
	ErrNoHealthyObservers     = &ObserverError{"no healthy observers available"}
	ErrInsufficientSyncReplicas = &ObserverError{"insufficient sync replicas"}
	ErrObserverLagTooBig      = &ObserverError{"observer lag too large"}
)

// ObserverError represents an observer-related error
type ObserverError struct {
	Message string
}

func (e *ObserverError) Error() string {
	return e.Message
}
