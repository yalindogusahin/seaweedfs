# SeaweedFS Observer Pattern Design

## Overview

This document describes the implementation of an **Observer Pattern** for SeaweedFS, inspired by Kafka's multi-region observer design. Observers enable efficient cross-datacenter replication by providing **asynchronous replication** without impacting write acknowledgment latency.

## Motivation

In multi-datacenter deployments, traditional synchronous replication requires waiting for all replicas to acknowledge writes before confirming to the client. This causes:

1. **High latency** - Write acknowledgments must traverse inter-datacenter network
2. **Throughput limitations** - Network congestion affects all writes
3. **Availability issues** - Cross-DC network partitions can make volumes unwritable

**Observers solve this by:**
- Keeping up with leader replicas asynchronously (like followers)
- **Not** joining the synchronous replica quorum
- Not blocking write acknowledgments
- Can be promoted to synchronous replicas when needed

## Architecture

### Replica Types

| Type | Role | Ack Behavior |
|------|------|--------------|
| **Leader** | Primary write target | Handles all writes |
| **Synchronous Follower** | Full replica in quorum | Must acknowledge for write success |
| **Observer** | Async replica only | Does NOT affect write acks |

### Replication Flow

```
┌─────────────┐     sync ack     ┌─────────────┐
│   Client    │ ◄─────────────── │   Leader    │
└─────────────┘                  └──────┬──────┘
                                        │
                          ┌─────────────┼─────────────┐
                          │             │             │
                     sync │        sync │        async│
                     ack  │        ack  │        sync │
                          ▼             ▼             ▼
                   ┌──────────┐  ┌──────────┐  ┌──────────┐
                   │Follower 1│  │Follower 2│  │Observer  │
                   └──────────┘  └──────────┘  └──────────┘
```

## Configuration

### Extended Replica Placement

The current replica placement format is `DRS` (DataCenter-Rack-SameRack). We extend it to support observers:

```
Format: DRS+ where:
- D: Number of replicas in different data centers (synchronous)
- R: Number of replicas in different racks (synchronous)  
- S: Number of replicas in same rack (synchronous)
- +: Optional observer count suffix

Examples:
- "001" = 1 replica (leader only, no sync replication)
- "002" = 2 replicas (leader + 1 sync follower)
- "100" = 2 replicas across data centers (leader + 1 sync in different DC)
- "100+1" = 2 sync replicas + 1 observer in different DC
- "001+2" = 1 sync replica + 2 observers in same DC
```

### Observer Configuration Options

```go
type ObserverConfig struct {
    // Enable observer mode
    Enabled bool `json:"enabled"`
    
    // Maximum lag allowed before observer is considered unhealthy (seconds)
    MaxLagSeconds int64 `json:"max_lag_seconds"`
    
    // Minimum observers required for read operations
    MinObservers int `json:"min_observers"`
    
    // Auto-promotion settings
    AutoPromotion ObserverAutoPromotion `json:"auto_promotion"`
}

type ObserverAutoPromotion struct {
    // Enable automatic promotion when sync replicas fail
    Enabled bool `json:"enabled"`
    
    // Minimum sync replicas required before promoting
    MinSyncReplicas int `json:"min_sync_replicas"`
    
    // Lag threshold for promotion (if sync replicas are severely behind)
    PromoteOnLagSeconds int64 `json:"promote_on_lag_seconds"`
}
```

## Data Structures

### Volume State

```go
type VolumeReplicaType int

const (
    ReplicaType_Sync VolumeReplicaType = iota
    ReplicaType_Observer
)

type VolumeReplica struct {
    VolumeId   uint32
    ServerId   string
    ServerAddr string
    ReplicaType VolumeReplicaType
    LastSyncNs int64    // Last sync timestamp for observers
    IsHealthy  bool
}
```

### Master Side Tracking

```go
type VolumeLayout struct {
    // Existing fields...
    rp *super_block.ReplicaPlacement
    
    // New: Track observer locations per volume
    observerLocations map[needle.VolumeId][]*DataNode
}
```

## Implementation Components

### 1. Master Server Changes

- **Volume Assignment**: When assigning volume replicas, distinguish between sync and observer placements
- **Health Monitoring**: Track observer sync lag
- **Auto-Promotion**: Promote observers when sync replica count drops below threshold

### 2. Volume Server Changes

- **Observer Mode Flag**: Volume servers can be configured as observer-only nodes
- **Async Replication**: Pull-based replication from leader to observer
- **Read Routing**: Clients can read from observers (with awareness of potential staleness)

### 3. Replication Sync Service

```go
type ObserverSyncService struct {
    // Source leader volume server
    LeaderAddr string
    
    // Target observer volume server
    ObserverAddr string
    
    // Volume being synced
    VolumeId uint32
    
    // Last acknowledged sequence
    LastSeq uint64
    
    // Sync status
    Status ObserverSyncStatus
}
```

## API Extensions

### gRPC Protobuf Changes

```protobuf
// Add to volume_server.proto
enum ReplicaRole {
    REPLICA_ROLE_LEADER = 0;
    REPLICA_ROLE_SYNC_FOLLOWER = 1;
    REPLICA_ROLE_OBSERVER = 2;
}

message VolumeConfiguration {
    uint32 volume_id = 1;
    ReplicaRole role = 2;
    int64 last_sync_ns = 3;
}

// Add to master.proto
message AssignObserverRequest {
    uint32 volume_id = 1;
    string observer_server = 2;
    string sync_source = 3;
}

message ObserverStatus {
    string observer_server = 1;
    uint32 volume_id = 2;
    int64 last_sync_ns = 3;
    int64 lag_seconds = 4;
    bool is_healthy = 5;
}
```

### CLI Commands

```bash
# Configure a volume to have observers
weed shell volume.configure.observers -volumeId=123 -observers=2

# List observer status
weed shell volume.observers.status -volumeId=123

# Promote observer to sync replica
weed shell volume.observer.promote -volumeId=123 -server=observer-1

# Demote sync replica to observer
weed shell volume.observer.demote -volumeId=123 -server=sync-1
```

## Use Cases

### 1. Multi-Region File Access

Deploy observers in remote regions for faster read access:
- Primary region: 2-3 sync replicas
- Remote regions: 1 observer each
- Reads from observer are local, writes go to primary

### 2. Disaster Recovery

Maintain async observers in a separate region:
- Sync replicas in primary region for performance
- Observer in DR region for data safety
- Auto-promote on primary region failure

### 3. Bandwidth Optimization

Reduce cross-DC write traffic:
- Only replicate sync replicas synchronously
- Observer replication can be throttled or batched

## Safety Considerations

### Data Consistency

- Observers may lag behind sync replicas
- Read-from-observer should be explicit (not default behavior)
- Application should be aware of potential stale reads

### Promotion Safety

- Before promoting observer, verify it has all data
- May need to wait for catch-up before promotion
- Consider using read-only mode during transition

### Network Partitions

- During partition, observers remain available for reads
- Sync replicas continue operating normally
- Auto-promotion can restore quorum if needed

## Migration Path

1. **Phase 1**: Add data structures and protobuf definitions
2. **Phase 2**: Implement observer assignment in master
3. **Phase 3**: Implement async replication sync mechanism
4. **Phase 4**: Add auto-promotion logic
5. **Phase 5**: Add CLI and monitoring tools
6. **Phase 6**: Add read-from-observer support

## References

- [Kafka Multi-Region Observer Documentation](https://docs.confluent.io/platform/current/multi-dc-deployments/multi-region.html#observers)
- SeaweedFS Replication: `weed/storage/super_block/replica_placement.go`
- SeaweedFS Volume Layout: `weed/topology/volume_layout.go`
