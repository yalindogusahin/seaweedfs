package weed_server

import (
	"context"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/observer"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

// ObserverStatus returns the status of observers for a volume
func (ms *MasterServer) ObserverStatus(ctx context.Context, req *master_pb.ObserverStatusRequest) (*master_pb.ObserverStatusResponse, error) {
	vid := needle.VolumeId(req.VolumeId)

	// Get observer manager from master server (would need to be added to MasterServer struct)
	// For now, return a placeholder response
	resp := &master_pb.ObserverStatusResponse{
		VolumeId: req.VolumeId,
		Observers: []*master_pb.ObserverInfo{},
	}

	// Check if observer manager exists
	if ms.observerManager == nil {
		resp.Error = "observer manager not configured"
		return resp, nil
	}

	// Get observer statuses
	statuses := ms.observerManager.GetObserverStatuses(vid)
	if statuses == nil {
		resp.Error = "no observers configured for this volume"
		return resp, nil
	}

	for addr, status := range statuses {
		resp.Observers = append(resp.Observers, &master_pb.ObserverInfo{
			ObserverServer: status.ObserverServer,
			LeaderServer:   status.LeaderServer,
			LastSyncNs:     status.LastSyncNs,
			LagSeconds:     status.LagSeconds,
			IsHealthy:      status.IsHealthy,
			LastError:      status.LastError,
			Role:           master_pb.ReplicaRole_OBSERVER_ROLE_OBSERVER,
		})
	}

	// Get sync replica count from volume layout
	vl := ms.getVolumeLayoutForObserver(vid)
	if vl != nil {
		resp.SyncReplicaCount = int32(vl.GetSyncReplicaCount())
		resp.ObserverCount = int32(vl.GetObserverCount())
	}

	return resp, nil
}

// AssignObserver assigns observer replicas to a volume
func (ms *MasterServer) AssignObserver(ctx context.Context, req *master_pb.AssignObserverRequest) (*master_pb.AssignObserverResponse, error) {
	resp := &master_pb.AssignObserverResponse{
		VolumeId: req.VolumeId,
	}

	if ms.observerManager == nil {
		resp.Error = "observer manager not configured"
		return resp, nil
	}

	// Parse leader and sync replica addresses
	leaderAddr := req.LeaderServer
	syncReplicas := req.SyncReplicas

	// Convert available servers to pb.ServerAddress format
	availableServers := make([]pb.ServerAddress, 0, len(req.AvailableServers))
	for _, addr := range req.AvailableServers {
		availableServers = append(availableServers, pb.NewServerAddressFromString(addr))
	}

	// Assign observers
	observerServers, err := ms.observerManager.AssignObservers(
		needle.VolumeId(req.VolumeId),
		pb.NewServerAddressFromString(leaderAddr),
		syncReplicas,
		int(req.ObserverCount),
		availableServers,
	)

	if err != nil {
		resp.Error = err.Error()
		return resp, nil
	}

	// Convert to string addresses
	for _, addr := range observerServers {
		resp.ObserverServers = append(resp.ObserverServers, addr.String())
	}

	glog.V(0).Infof("Assigned %d observers to volume %d: %v", len(resp.ObserverServers), req.VolumeId, resp.ObserverServers)
	return resp, nil
}

// PromoteObserver promotes an observer to a sync replica
func (ms *MasterServer) PromoteObserver(ctx context.Context, req *master_pb.PromoteObserverRequest) (*master_pb.PromoteObserverResponse, error) {
	resp := &master_pb.PromoteObserverResponse{}

	if ms.observerManager == nil {
		resp.Error = "observer manager not configured"
		return resp, nil
	}

	// Check if observer is healthy enough for promotion
	status, err := ms.observerManager.GetObserverStatus(
		needle.VolumeId(req.VolumeId),
		req.ObserverServer,
	)

	if err != nil {
		resp.Error = err.Error()
		return resp, nil
	}

	// Check lag unless force is specified
	if !req.Force && !status.IsHealthy {
		resp.Error = "observer is not healthy, use -force to override"
		return resp, nil
	}

	// Promote the observer
	err = ms.observerManager.PromoteObserver(
		needle.VolumeId(req.VolumeId),
		req.ObserverServer,
	)

	if err != nil {
		resp.Error = err.Error()
		return resp, nil
	}

	// Update volume layout to reflect the promotion
	vl := ms.getVolumeLayoutForObserver(needle.VolumeId(req.VolumeId))
	if vl != nil {
		// Increment sync replica count, decrement observer count
		if vl.ExtendedRp != nil {
			vl.ExtendedRp.ObserverCount--
			// Note: Actual sync replica assignment would need topology integration
		}
	}

	glog.V(0).Infof("Promoted observer %s to sync replica for volume %d", req.ObserverServer, req.VolumeId)
	resp.Success = true
	return resp, nil
}

// DemoteReplica demotes a sync replica to an observer
func (ms *MasterServer) DemoteReplica(ctx context.Context, req *master_pb.DemoteReplicaRequest) (*master_pb.DemoteReplicaResponse, error) {
	resp := &master_pb.DemoteReplicaResponse{}

	if ms.observerManager == nil {
		resp.Error = "observer manager not configured"
		return resp, nil
	}

	// Get current sync replicas for the volume
	vl := ms.getVolumeLayoutForObserver(needle.VolumeId(req.VolumeId))
	if vl == nil {
		resp.Error = "volume not found"
		return resp, nil
	}

	// Check if this is actually a sync replica (not already an observer)
	// This would need proper tracking of replica roles
	// For now, just register it as an observer
	leaderAddr := ms.Topo.GetMasterAddress() // Get master as leader for now

	err := ms.observerManager.RegisterObserver(
		needle.VolumeId(req.VolumeId),
		req.ReplicaServer,
		leaderAddr.String(),
	)

	if err != nil {
		resp.Error = err.Error()
		return resp, nil
	}

	// Update volume layout
	if vl.ExtendedRp != nil {
		vl.ExtendedRp.ObserverCount++
		// Note: Actual sync replica removal would need topology integration
	}

	glog.V(0).Infof("Demoted replica %s to observer for volume %d", req.ReplicaServer, req.VolumeId)
	resp.Success = true
	return resp, nil
}

// getVolumeLayoutForObserver gets the volume layout for a volume (helper method)
func (ms *MasterServer) getVolumeLayoutForObserver(vid needle.VolumeId) *observer.VolumeLayoutExt {
	// This would need to be implemented to look up the volume layout
	// from the topology for the given volume ID
	// For now, return nil
	return nil
}
