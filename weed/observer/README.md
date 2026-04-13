# SeaweedFS Observer Pattern Implementation

This document describes the implementation of the Observer Pattern for SeaweedFS, inspired by Kafka's multi-region observer design.

## Overview

Observers enable efficient cross-datacenter replication by providing **asynchronous replication** without impacting write acknowledgment latency. This is particularly useful for:

- Multi-region file access with local read performance
- Disaster recovery with async replicas in remote regions
- Bandwidth optimization by reducing cross-DC write traffic

## Key Concepts

### Replica Types

| Type | Role | Ack Behavior |
|------|------|--------------|
| **Leader** | Primary write target | Handles all writes |
| **Synchronous Follower** | Full replica in quorum | Must acknowledge for write success |
| **Observer** | Async replica only | Does NOT affect write acks |

### Replication Configuration Format

The extended replication format is `DRS+O` where:
- **D**: Number of replicas in different data centers (synchronous)
- **R**: Number of replicas in different racks (synchronous)
- **S**: Number of replicas in same rack (synchronous)
- **O**: Number of observer replicas (asynchronous)

Examples:
- `001` = 1 replica (leader only)
- `002` = 2 replicas (leader + 1 sync follower)
- `100` = 2 replicas across data centers
- `100+1` = 2 sync replicas + 1 observer in different DC
- `001+2` = 1 sync replica + 2 observers in same DC

## Implementation Components

### 1. Core Types (`weed/observer/types.go`)

```go
// ReplicaRole defines the role of a volume replica
type ReplicaRole int

const (
    ReplicaRoleLeader       // Primary
    ReplicaRoleSyncFollower // Sync replica
    ReplicaRoleObserver     // Async observer
)

// ObserverConfig holds observer configuration
type ObserverConfig struct {
    Enabled         bool
    MaxLagSeconds   int64
    MinObservers    int
    AutoPromotion   ObserverAutoPromotion
}
```

### 2. Observer Manager (`weed/observer/manager.go`)

Manages observer lifecycle, health monitoring, and auto-promotion:

```go
manager := observer.NewManager(observer.DefaultObserverConfig())
manager.Start(context.Background())

// Register an observer
manager.RegisterObserver(volumeId, "observer-1:8080", "leader-1:8080")

// Get healthy observers
healthy := manager.GetHealthyObservers(volumeId)
```

### 3. Sync Service (`weed/observer/sync_service.go`)

Handles asynchronous replication from leader to observers:

```go
service := observer.NewObserverSyncService(
    "leader-1:8080",
    "observer-1:8080",
    volumeId,
)
service.Start()
```

### 4. Volume Layout Extension (`weed/observer/assignment.go`)

Extends volume layout with observer support:

```go
erp, err := observer.NewExtendedReplicaPlacement("100+2")
if err != nil {
    // handle error
}

extLayout := observer.NewVolumeLayoutExt(
    volumeLayout,
    manager,
    erp,
)

// Assign observers for a new volume
observerNodes, err := extLayout.AssignObservers(
    volumeId,
    leaderNode,
    syncReplicaNodes,
)
```

### 5. CLI Commands (`weed/shell/command_observer.go`)

Available shell commands:

```bash
# Display observer status
weed shell observer.status -volumeId=123
weed shell observer.status -all

# Assign observers to a volume
weed shell observer.assign -volumeId=123 -observers=2

# Promote observer to sync replica
weed shell observer.promote -volumeId=123 -server=observer-1
weed shell observer.promote -volumeId=123 -server=observer-1 -force

# Demote sync replica to observer
weed shell observer.demote -volumeId=123 -server=sync-1

# Configure observer settings
weed shell observer.configure -volumeId=123 -replication=100+2
```

### 6. gRPC API Extensions

#### master.proto
```protobuf
// Observer-related RPCs
rpc AssignObserver (AssignObserverRequest) returns (AssignObserverResponse)
rpc ObserverStatus (ObserverStatusRequest) returns (ObserverStatusResponse)
rpc PromoteObserver (PromoteObserverRequest) returns (PromoteObserverResponse)
rpc DemoteReplica (DemoteReplicaRequest) returns (DemoteReplicaResponse)
```

#### volume_server.proto
```protobuf
// Observer sync RPCs
rpc SyncObserverShard (SyncObserverShardRequest) returns (SyncObserverShardResponse)
rpc ApplyShardUpdates (ApplyShardUpdatesRequest) returns (ApplyShardUpdatesResponse)
rpc GetObserverStatus (GetObserverStatusRequest) returns (GetObserverStatusResponse)
```

## Usage Examples

### Setting up Multi-Region Replication with Observers

1. **Configure master with observer support:**
```toml
# master.toml
[observer]
enabled = true
max_lag_seconds = 300
```

2. **Create a volume with observers:**
```bash
# Create volume with 1 sync replica in different DC and 2 observers
weed shell volume.create -replication=100+2 -collection=mydata
```

3. **Monitor observer status:**
```bash
weed shell observer.status -volumeId=1
```

4. **Promote observer during failover:**
```bash
# If sync replica becomes unavailable, promote observer
weed shell observer.promote -volumeId=1 -server=observer-uswest -force
```

### Disaster Recovery Setup

```bash
# Primary region: 2 sync replicas
# DR region: 1 observer
weed shell volume.configure.replication -volumeId=1 -replication=002+1
weed shell observer.assign -volumeId=1 -observers=1
```

## Auto-Promotion

Observers can be automatically promoted to sync replicas when:

1. Sync replica count drops below minimum
2. Observer lag exceeds threshold (configurable)

Configuration:
```go
config := observer.DefaultObserverConfig()
config.AutoPromotion = observer.ObserverAutoPromotion{
    Enabled:             true,
    MinSyncReplicas:     1,
    PromoteOnLagSeconds: 60,
}
```

## Safety Considerations

### Data Consistency
- Observers may lag behind sync replicas
- Reads from observers may return stale data
- Applications should be aware of potential staleness

### Promotion Safety
- Verify observer has caught up before promoting
- Use `-force` flag only when necessary
- Monitor replication lag during promotion

### Network Partitions
- Observers remain available for reads during partitions
- Sync replicas continue operating normally
- Auto-promotion can restore quorum if needed

## Future Enhancements

1. **Read-from-Observer Support**: Enable clients to read from observers with staleness awareness
2. **Observer Health Metrics**: Prometheus metrics for observer lag and health
3. **Automatic Failover**: Full automatic failover to observers
4. **Cross-Region Bandwidth Throttling**: Limit observer sync bandwidth usage
5. **Observer Grouping**: Group observers by region for efficient management

## References

- [Kafka Multi-Region Observer Documentation](https://docs.confluent.io/platform/current/multi-dc-deployments/multi-region.html#observers)
- [Design Document](OBSERVER_DESIGN.md)
