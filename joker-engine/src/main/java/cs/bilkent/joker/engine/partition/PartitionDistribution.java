package cs.bilkent.joker.engine.partition;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.unmodifiableList;

public class PartitionDistribution
{

    private final int[] distribution;

    private final Map<Integer, List<Integer>> partitionIdsByReplicaIndex;

    public PartitionDistribution ( final int[] distribution )
    {
        this.distribution = Arrays.copyOf( distribution, distribution.length );
        final Map<Integer, List<Integer>> partitionIdsByReplicaIndex = new HashMap<>();
        for ( int partitionId = 0; partitionId < distribution.length; partitionId++ )
        {
            final int replicaIndex = distribution[ partitionId ];
            partitionIdsByReplicaIndex.computeIfAbsent( replicaIndex, ArrayList::new ).add( partitionId );
        }

        for ( Integer replicaIndex : new ArrayList<>( partitionIdsByReplicaIndex.keySet() ) )
        {
            final List<Integer> partitionIds = partitionIdsByReplicaIndex.get( replicaIndex );
            partitionIdsByReplicaIndex.put( replicaIndex, unmodifiableList( partitionIds ) );
        }

        this.partitionIdsByReplicaIndex = Collections.unmodifiableMap( partitionIdsByReplicaIndex );
    }

    public int[] getDistribution ()
    {
        return Arrays.copyOf( distribution, distribution.length );
    }

    public int getPartitionCount ()
    {
        return distribution.length;
    }

    public int getReplicaCount ()
    {
        return partitionIdsByReplicaIndex.size();
    }

    public List<Integer> getPartitionsByReplicaIndex ( final int replicaIndex )
    {
        return partitionIdsByReplicaIndex.get( replicaIndex );
    }

    public List<PartitionOwnershipChange> getPartitionsMovedToReplicaIndex ( final PartitionDistribution newDistribution,
                                                                             final int replicaIndex )
    {
        final List<PartitionOwnershipChange> changes = new ArrayList<>();

        for ( int partitionId = 0; partitionId < getPartitionCount(); partitionId++ )
        {
            final int oldReplicaIndex = this.distribution[ partitionId ];
            final int newReplicaIndex = newDistribution.distribution[ partitionId ];
            if ( oldReplicaIndex != replicaIndex && newReplicaIndex == replicaIndex )
            {
                changes.add( new PartitionOwnershipChange( partitionId, oldReplicaIndex, newReplicaIndex ) );
            }
        }

        return changes;
    }

    public List<PartitionOwnershipChange> getPartitionsMovedFromReplicaIndex ( final PartitionDistribution newDistribution,
                                                                               final int replicaIndex )
    {
        final List<PartitionOwnershipChange> changes = new ArrayList<>();

        for ( int partitionId = 0; partitionId < getPartitionCount(); partitionId++ )
        {
            final int oldReplicaIndex = this.distribution[ partitionId ];
            final int newReplicaIndex = newDistribution.distribution[ partitionId ];
            if ( oldReplicaIndex == replicaIndex && newReplicaIndex != replicaIndex )
            {
                changes.add( new PartitionOwnershipChange( partitionId, oldReplicaIndex, newReplicaIndex ) );
            }
        }

        return changes;
    }

    public List<PartitionOwnershipChange> diff ( final PartitionDistribution newDistribution )
    {
        final List<PartitionOwnershipChange> changes = new ArrayList<>();

        for ( int partitionId = 0; partitionId < getPartitionCount(); partitionId++ )
        {
            final int oldReplicaIndex = this.distribution[ partitionId ];
            final int newReplicaIndex = newDistribution.distribution[ partitionId ];
            if ( oldReplicaIndex != newReplicaIndex )
            {
                changes.add( new PartitionOwnershipChange( partitionId, oldReplicaIndex, newReplicaIndex ) );
            }
        }

        return changes;
    }

    public static class PartitionOwnershipChange
    {

        public final int partitionId;

        public final int oldReplicaIndex;

        public final int newReplicaIndex;

        private PartitionOwnershipChange ( final int partitionId, final int oldReplicaIndex, final int newReplicaIndex )
        {
            this.partitionId = partitionId;
            this.oldReplicaIndex = oldReplicaIndex;
            this.newReplicaIndex = newReplicaIndex;
        }

    }

}
