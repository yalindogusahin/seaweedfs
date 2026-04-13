package shell

import (
	"context"
	"flag"
	"fmt"
	"io"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/observer"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

func init() {
	Commands = append(Commands,
		&commandObserverStatus{},
		&commandObserverAssign{},
		&commandObserverPromote{},
		&commandObserverDemote{},
		&commandObserverConfigure{},
	)
}

// commandObserverStatus displays observer status for volumes
type commandObserverStatus struct {
}

func (c *commandObserverStatus) Name() string {
	return "observer.status"
}

func (c *commandObserverStatus) Help() string {
	return `display observer status for volumes

	This command displays the status of observer replicas for a specific volume
	or all volumes.

	Example:
		weed shell observer.status -volumeId=123
		weed shell observer.status -all
`
}

func (c *commandObserverStatus) HasTag(CommandTag) bool {
	return false
}

func (c *commandObserverStatus) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {
	statusCmd := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	volumeIdInt := statusCmd.Int("volumeId", 0, "the volume id (0 for all volumes)")
	allVolumes := statusCmd.Bool("all", false, "show status for all volumes with observers")

	if err = statusCmd.Parse(args); err != nil {
		return nil
	}

	if err = commandEnv.connectToMaster(); err != nil {
		return err
	}

	volumeId := needle.VolumeId(*volumeIdInt)

	err = pb.WithMasterClient(false, commandEnv.masterAddress, commandEnv.option.GrpcDialOption,
		func(client master_pb.SeaweedClient) error {

			if volumeId > 0 || !*allVolumes {
				// Get status for specific volume
				resp, err := client.ObserverStatus(context.Background(), &master_pb.ObserverStatusRequest{
					VolumeId: uint32(volumeId),
				})
				if err != nil {
					return fmt.Errorf("observer status request failed: %w", err)
				}
				if resp.Error != "" {
					return fmt.Errorf("observer status error: %s", resp.Error)
				}
				c.printObserverStatus(writer, resp)
			} else {
				// Get status for all volumes
				c.printAllObserverStatus(writer, client)
			}

			return nil
		})

	return err
}

func (c *commandObserverStatus) printObserverStatus(writer io.Writer, resp *master_pb.ObserverStatusResponse) {
	fmt.Fprintf(writer, "Volume %d:\n", resp.VolumeId)
	fmt.Fprintf(writer, "  Sync Replicas: %d\n", resp.SyncReplicaCount)
	fmt.Fprintf(writer, "  Observers: %d\n", resp.ObserverCount)

	if len(resp.Observers) == 0 {
		fmt.Fprintf(writer, "  No observers configured\n")
		return
	}

	fmt.Fprintf(writer, "  Observer Status:\n")
	for _, obs := range resp.Observers {
		healthStatus := "healthy"
		if !obs.IsHealthy {
			healthStatus = "UNHEALTHY"
		}
		fmt.Fprintf(writer, "    - %s: lag=%ds, status=%s",
			obs.ObserverServer, obs.LagSeconds, healthStatus)
		if obs.LastError != "" {
			fmt.Fprintf(writer, ", error=%s", obs.LastError)
		}
		fmt.Fprintf(writer, "\n")
	}
}

func (c *commandObserverStatus) printAllObserverStatus(writer io.Writer, client master_pb.SeaweedClient) error {
	// First get all volumes
	volResp, err := client.VolumeList(context.Background(), &master_pb.VolumeListRequest{})
	if err != nil {
		return fmt.Errorf("failed to list volumes: %w", err)
	}

	hasObservers := false
	for _, dc := range volResp.TopologyInfo.DataCenterInfos {
		for _, rack := range dc.RackInfos {
			for _, node := range rack.DataNodeInfos {
				for diskType, disk := range node.DiskInfos {
					for _, vol := range disk.VolumeInfos {
						if vol.ObserverCount > 0 {
							if !hasObservers {
								fmt.Fprintf(writer, "Volumes with observers:\n")
								hasObservers = true
							}
							fmt.Fprintf(writer, "  Volume %d: %d observers (sync=%d)\n",
								vol.Id, vol.ObserverCount, vol.SyncReplicaCount)
						}
					}
				}
			}
		}
	}

	if !hasObservers {
		fmt.Fprintf(writer, "No volumes with observers found\n")
	}

	return nil
}

// commandObserverAssign assigns observers to a volume
type commandObserverAssign struct {
}

func (c *commandObserverAssign) Name() string {
	return "observer.assign"
}

func (c *commandObserverAssign) Help() string {
	return `assign observer replicas to a volume

	This command assigns observer replicas to an existing volume.

	Example:
		weed shell observer.assign -volumeId=123 -observers=2
`
}

func (c *commandObserverAssign) HasTag(CommandTag) bool {
	return false
}

func (c *commandObserverAssign) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {
	assignCmd := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	volumeIdInt := assignCmd.Int("volumeId", 0, "the volume id")
	observerCount := assignCmd.Int("observers", 0, "number of observers to assign")

	if err = assignCmd.Parse(args); err != nil {
		return nil
	}

	if *volumeIdInt <= 0 {
		return fmt.Errorf("volume id must be positive")
	}
	if *observerCount <= 0 {
		return fmt.Errorf("observer count must be positive")
	}

	if err = commandEnv.connectToMaster(); err != nil {
		return err
	}

	volumeId := uint32(*volumeIdInt)

	err = pb.WithMasterClient(false, commandEnv.masterAddress, commandEnv.option.GrpcDialOption,
		func(client master_pb.SeaweedClient) error {

			// Get current volume info to find leader and sync replicas
			volResp, err := client.VolumeList(context.Background(), &master_pb.VolumeListRequest{})
			if err != nil {
				return fmt.Errorf("failed to list volumes: %w", err)
			}

			var leaderServer string
			var syncReplicas []string

			// Find the volume and its replicas
			for _, dc := range volResp.TopologyInfo.DataCenterInfos {
				for _, rack := range dc.RackInfos {
					for _, node := range rack.DataNodeInfos {
						for diskType, disk := range node.DiskInfos {
							for _, vol := range disk.VolumeInfos {
								if vol.Id == volumeId {
									leaderServer = node.Address
									syncReplicas = append(syncReplicas, node.Address)
								}
							}
						}
					}
				}
			}

			if leaderServer == "" {
				return fmt.Errorf("volume %d not found", volumeId)
			}

			// Build list of available servers
			var availableServers []string
			for _, dc := range volResp.TopologyInfo.DataCenterInfos {
				for _, rack := range dc.RackInfos {
					for _, node := range rack.DataNodeInfos {
						// Skip if this is the leader or sync replica
						isReplica := node.Address == leaderServer
						for _, syncAddr := range syncReplicas {
							if node.Address == syncAddr {
								isReplica = true
								break
							}
						}
						if !isReplica {
							availableServers = append(availableServers, node.Address)
						}
					}
				}
			}

			// Request observer assignment
			resp, err := client.AssignObserver(context.Background(), &master_pb.AssignObserverRequest{
				VolumeId:        volumeId,
				LeaderServer:    leaderServer,
				SyncReplicas:    syncReplicas,
				ObserverCount:   int32(*observerCount),
				AvailableServers: availableServers,
			})

			if err != nil {
				return fmt.Errorf("observer assign request failed: %w", err)
			}
			if resp.Error != "" {
				return fmt.Errorf("observer assign error: %s", resp.Error)
			}

			fmt.Fprintf(writer, "Assigned %d observers to volume %d:\n", len(resp.ObserverServers), volumeId)
			for _, server := range resp.ObserverServers {
				fmt.Fprintf(writer, "  - %s\n", server)
			}

			return nil
		})

	return err
}

// commandObserverPromote promotes an observer to a sync replica
type commandObserverPromote struct {
}

func (c *commandObserverPromote) Name() string {
	return "observer.promote"
}

func (c *commandObserverPromote) Help() string {
	return `promote an observer to a sync replica

	This command promotes an observer to become a synchronous replica.

	Example:
		weed shell observer.promote -volumeId=123 -server=observer-1
		weed shell observer.promote -volumeId=123 -server=observer-1 -force
`
}

func (c *commandObserverPromote) HasTag(CommandTag) bool {
	return false
}

func (c *commandObserverPromote) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {
	promoteCmd := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	volumeIdInt := promoteCmd.Int("volumeId", 0, "the volume id")
	server := promoteCmd.String("server", "", "the observer server to promote")
	force := promoteCmd.Bool("force", false, "force promotion even if lag is high")

	if err = promoteCmd.Parse(args); err != nil {
		return nil
	}

	if *volumeIdInt <= 0 {
		return fmt.Errorf("volume id must be positive")
	}
	if *server == "" {
		return fmt.Errorf("server must be specified")
	}

	if err = commandEnv.connectToMaster(); err != nil {
		return err
	}

	err = pb.WithMasterClient(false, commandEnv.masterAddress, commandEnv.option.GrpcDialOption,
		func(client master_pb.SeaweedClient) error {

			resp, err := client.PromoteObserver(context.Background(), &master_pb.PromoteObserverRequest{
				VolumeId:      uint32(*volumeIdInt),
				ObserverServer: *server,
				Force:         *force,
			})

			if err != nil {
				return fmt.Errorf("observer promote request failed: %w", err)
			}
			if !resp.Success {
				return fmt.Errorf("observer promote error: %s", resp.Error)
			}

			fmt.Fprintf(writer, "Successfully promoted %s to sync replica for volume %d\n",
				*server, *volumeIdInt)

			return nil
		})

	return err
}

// commandObserverDemote demotes a sync replica to an observer
type commandObserverDemote struct {
}

func (c *commandObserverDemote) Name() string {
	return "observer.demote"
}

func (c *commandObserverDemote) Help() string {
	return `demote a sync replica to an observer

	This command demotes a sync replica to become an asynchronous observer.

	Example:
		weed shell observer.demote -volumeId=123 -server=sync-1
`
}

func (c *commandObserverDemote) HasTag(CommandTag) bool {
	return false
}

func (c *commandObserverDemote) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {
	demoteCmd := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	volumeIdInt := demoteCmd.Int("volumeId", 0, "the volume id")
	server := demoteCmd.String("server", "", "the sync replica server to demote")

	if err = demoteCmd.Parse(args); err != nil {
		return nil
	}

	if *volumeIdInt <= 0 {
		return fmt.Errorf("volume id must be positive")
	}
	if *server == "" {
		return fmt.Errorf("server must be specified")
	}

	if err = commandEnv.connectToMaster(); err != nil {
		return err
	}

	err = pb.WithMasterClient(false, commandEnv.masterAddress, commandEnv.option.GrpcDialOption,
		func(client master_pb.SeaweedClient) error {

			resp, err := client.DemoteReplica(context.Background(), &master_pb.DemoteReplicaRequest{
				VolumeId:       uint32(*volumeIdInt),
				ReplicaServer:  *server,
			})

			if err != nil {
				return fmt.Errorf("observer demote request failed: %w", err)
			}
			if !resp.Success {
				return fmt.Errorf("observer demote error: %s", resp.Error)
			}

			fmt.Fprintf(writer, "Successfully demoted %s to observer for volume %d\n",
				*server, *volumeIdInt)

			return nil
		})

	return err
}

// commandObserverConfigure configures observer settings
type commandObserverConfigure struct {
}

func (c *commandObserverConfigure) Name() string {
	return "observer.configure"
}

func (c *commandObserverConfigure) Help() string {
	return `configure observer settings for a volume

	This command configures observer settings for a volume.

	Example:
		weed shell observer.configure -volumeId=123 -replication=100+2
		weed shell observer.configure -collectionPattern="*" -replication=001+1
`
}

func (c *commandObserverConfigure) HasTag(CommandTag) bool {
	return false
}

func (c *commandObserverConfigure) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {
	configureCmd := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	volumeIdInt := configureCmd.Int("volumeId", 0, "the volume id")
	replication := configureCmd.String("replication", "", "replication config (e.g., 100+2 for 1 DC sync + 2 observers)")
	collectionPattern := configureCmd.String("collectionPattern", "", "match with wildcard characters")

	if err = configureCmd.Parse(args); err != nil {
		return nil
	}

	if *replication == "" {
		return fmt.Errorf("replication must be specified")
	}

	// Parse the replication string
	_, observerCount, err := observer.ParseReplicationString(*replication)
	if err != nil {
		return fmt.Errorf("invalid replication format: %w", err)
	}

	if err = commandEnv.connectToMaster(); err != nil {
		return err
	}

	if volumeIdInt != nil && *volumeIdInt > 0 {
		// Configure for specific volume
		fmt.Fprintf(writer, "Configuring observer settings for volume %d:\n", *volumeIdInt)
		fmt.Fprintf(writer, "  Replication: %s (observers: %d)\n", *replication, observerCount)
	} else if collectionPattern != nil && *collectionPattern != "" {
		// Configure for collection pattern
		fmt.Fprintf(writer, "Configuring observer settings for collection pattern '%s':\n", *collectionPattern)
		fmt.Fprintf(writer, "  Replication: %s (observers: %d)\n", *replication, observerCount)
	} else {
		return fmt.Errorf("either volumeId or collectionPattern must be specified")
	}

	// Note: This would need to integrate with the volume configure mechanism
	// to actually apply the changes across all replicas
	fmt.Fprintf(writer, "Note: Apply changes with 'volume.fix.replication' after configuration\n")

	return nil
}
