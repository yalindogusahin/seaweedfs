package observer

import (
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/topology"
)

// VolumeLayoutExt extends the volume layout with observer support
type VolumeLayoutExt struct {
	// Embedded original volume layout
	*topology.VolumeLayout

	// Observer manager for tracking observers
	Manager *Manager

	// Extended replica placement
	ExtendedRp *ExtendedReplicaPlacement
}

// NewVolumeLayoutExt creates a new extended volume layout with observer support
func NewVolumeLayoutExt(vl *topology.VolumeLayout, manager *Manager, extendedRp *ExtendedReplicaPlacement) *VolumeLayoutExt {
	return &VolumeLayoutExt{
		VolumeLayout: vl,
		Manager:      manager,
		ExtendedRp:   extendedRp,
	}
}

// AssignObservers assigns observer replicas for a newly created volume
func (vle *VolumeLayoutExt) AssignObservers(
	vid needle.VolumeId,
	leaderNode *topology.DataNode,
	syncReplicaNodes []*topology.DataNode,
) ([]*topology.DataNode, error) {

	if vle.ExtendedRp == nil || vle.ExtendedRp.ObserverCount <= 0 {
		return nil, nil
	}

	// Build list of available servers (excluding leader and sync replicas)
	leaderAddr := leaderNode.ServerAddress().String()
	syncReplicaAddrs := make([]pb.ServerAddress, 0, len(syncReplicaNodes))
	for _, node := range syncReplicaNodes {
		syncReplicaAddrs = append(syncReplicaAddrs, node.ServerAddress())
	}

	// Get all available data nodes from topology
	allNodes := vle.Topology().ListVolumeServers()
	availableServers := make([]pb.ServerAddress, 0, len(allNodes))
	for _, node := range allNodes {
		addr := node.ServerAddress()
		// Skip if already assigned as leader or sync replica
		if addr.String() == leaderAddr {
			continue
		}
		isSyncReplica := false
		for _, syncAddr := range syncReplicaAddrs {
			if syncAddr.String() == addr.String() {
				isSyncReplica = true
				break
			}
		}
		if !isSyncReplica {
			availableServers = append(availableServers, addr)
		}
	}

	// Assign observers from available servers
	observerAddrs, err := vle.Manager.AssignObservers(vid, leaderNode.ServerAddress(), syncReplicaAddrs,
		vle.ExtendedRp.ObserverCount, availableServers)
	if err != nil {
		return nil, fmt.Errorf("failed to assign observers: %w", err)
	}

	// Convert to DataNode pointers
	var observerNodes []*topology.DataNode
	for _, addr := range observerAddrs {
		// Find the DataNode for this address
		node := findDataNodeByAddress(vle.Topology(), addr.String())
		if node != nil {
			observerNodes = append(observerNodes, node)
		}
	}

	glog.V(1).Infof("Assigned %d observers for volume %d: %v", len(observerNodes), vid, observerAddrs)
	return observerNodes, nil
}

// UpdateVolumeReplication updates the replication config for a volume
func (vle *VolumeLayoutExt) UpdateVolumeReplication(vid needle.VolumeId, newRp *super_block.ReplicaPlacement, observerCount int) error {
	if vle.ExtendedRp == nil {
		vle.ExtendedRp = &ExtendedReplicaPlacement{
			ReplicaPlacement: newRp,
			ObserverCount:    observerCount,
		}
	} else {
		vle.ExtendedRp.ReplicaPlacement = newRp
		vle.ExtendedRp.ObserverCount = observerCount
	}

	glog.V(1).Infof("Updated replication for volume %d: sync=%d, observers=%d",
		vid, newRp.GetCopyCount(), observerCount)
	return nil
}

// GetObserverCount returns the configured observer count for this volume layout
func (vle *VolumeLayoutExt) GetObserverCount() int {
	if vle.ExtendedRp == nil {
		return 0
	}
	return vle.ExtendedRp.ObserverCount
}

// GetSyncReplicaCount returns the configured sync replica count
func (vle *VolumeLayoutExt) GetSyncReplicaCount() int {
	if vle.ExtendedRp == nil {
		return 1 // Just the leader
	}
	return vle.ExtendedRp.GetSyncReplicaCount()
}

// ParseReplicationString parses a replication string that may include observers
// Format: "DRS" or "DRS+O" where D=DC, R=Rack, S=SameRack, O=Observer count
func ParseReplicationString(s string) (*super_block.ReplicaPlacement, int, error) {
	erp, err := NewExtendedReplicaPlacement(s)
	if err != nil {
		return nil, 0, err
	}
	return erp.ReplicaPlacement, erp.ObserverCount, nil
}

// ToExtendedReplicaPlacement converts a standard replica placement to extended format
func ToExtendedReplicaPlacement(rp *super_block.ReplicaPlacement, observerCount int) *ExtendedReplicaPlacement {
	return &ExtendedReplicaPlacement{
		ReplicaPlacement: rp,
		ObserverCount:    observerCount,
	}
}

// ToProto converts to protobuf format
func (erp *ExtendedReplicaPlacement) ToProto() *master_pb.ExtendedReplicaPlacement {
	if erp == nil {
		return nil
	}
	return &master_pb.ExtendedReplicaPlacement{
		DiffDataCenterCount: int32(erp.DiffDataCenterCount),
		DiffRackCount:       int32(erp.DiffRackCount),
		SameRackCount:       int32(erp.SameRackCount),
		ObserverCount:       int32(erp.ObserverCount),
	}
}

// FromProto creates from protobuf format
func FromProto(proto *master_pb.ExtendedReplicaPlacement) *ExtendedReplicaPlacement {
	if proto == nil {
		return nil
	}
	return &ExtendedReplicaPlacement{
		ReplicaPlacement: &super_block.ReplicaPlacement{
			DiffDataCenterCount: int(proto.DiffDataCenterCount),
			DiffRackCount:       int(proto.DiffRackCount),
			SameRackCount:       int(proto.SameRackCount),
		},
		ObserverCount: int(proto.ObserverCount),
	}
}

// findDataNodeByAddress finds a DataNode by its address string
func findDataNodeByAddress(topo *topology.Topology, addr string) *topology.DataNode {
	allNodes := topo.ListVolumeServers()
	for _, node := range allNodes {
		if node.Url() == addr || node.ServerAddress().String() == addr {
			return node
		}
	}
	return nil
}

// GetObserverLocations returns the observer locations for a volume
func (vle *VolumeLayoutExt) GetObserverLocations(vid needle.VolumeId) []*topology.DataNode {
	statuses := vle.Manager.GetObserverStatuses(vid)
	if statuses == nil {
		return nil
	}

	var nodes []*topology.DataNode
	for addr := range statuses {
		node := findDataNodeByAddress(vle.Topology(), addr)
		if node != nil {
			nodes = append(nodes, node)
		}
	}
	return nodes
}

// GetHealthyObserverCount returns the number of healthy observers
func (vle *VolumeLayoutExt) GetHealthyObserverCount(vid needle.VolumeId) int {
	healthy := vle.Manager.GetHealthyObservers(vid)
	return len(healthy)
}
