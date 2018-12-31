package cs.bilkent.joker.engine.partition;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
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
        checkArgument( this.partitionIdsByReplicaIndex.size() > 0, "no replica indices found in partition distribution!" );
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

    public int getReplicaIndex ( final int partitionId )
    {
        return distribution[ partitionId ];
    }

    public List<Integer> getPartitionIdsByReplicaIndex ( final int replicaIndex )
    {
        return partitionIdsByReplicaIndex.get( replicaIndex );
    }

    public List<Integer> getPartitionIdsMigratedToReplicaIndex ( final PartitionDistribution other, final int replicaIndex )
    {
        verifyReplicaCount( other );

        final List<Integer> migrations = new ArrayList<>();

        for ( int partitionId = 0; partitionId < getPartitionCount(); partitionId++ )
        {
            final int thisReplicaIndex = this.distribution[ partitionId ];
            final int otherReplicaIndex = other.distribution[ partitionId ];
            if ( thisReplicaIndex != replicaIndex && otherReplicaIndex == replicaIndex )
            {
                verifyMigrationReplicaIndices( other, thisReplicaIndex, otherReplicaIndex );
                migrations.add( partitionId );
            }
        }

        return migrations;
    }

    public List<Integer> getPartitionIdsMigratedFromReplicaIndex ( final PartitionDistribution other, final int replicaIndex )
    {
        verifyReplicaCount( other );

        final List<Integer> migrations = new ArrayList<>();

        for ( int partitionId = 0; partitionId < getPartitionCount(); partitionId++ )
        {
            final int thisReplicaIndex = this.distribution[ partitionId ];
            final int otherReplicaIndex = other.distribution[ partitionId ];
            if ( thisReplicaIndex == replicaIndex && otherReplicaIndex != replicaIndex )
            {
                verifyMigrationReplicaIndices( other, thisReplicaIndex, otherReplicaIndex );
                migrations.add( partitionId );
            }
        }

        return migrations;
    }

    private void verifyReplicaCount ( final PartitionDistribution other )
    {
        checkState( this.getReplicaCount() != other.getReplicaCount(),
                    "both distributions have same replica count! this: {} other: {}",
                    distribution,
                    other.distribution );
    }

    private void verifyMigrationReplicaIndices ( final PartitionDistribution other,
                                                 final int thisReplicaIndex,
                                                 final int otherReplicaIndex )
    {
        if ( otherReplicaIndex > thisReplicaIndex )
        {
            checkState( otherReplicaIndex >= this.getReplicaCount() );
        }
        else
        {
            checkState( thisReplicaIndex >= other.getReplicaCount() );
        }
    }

    @Override
    public String toString ()
    {
        final StringBuilder sb = new StringBuilder();
        sb.append( getClass().getSimpleName() ).append( "{" );

        for ( int i = 0; i < getReplicaCount(); i++ )
        {
            sb.append( "replica=" )
              .append( i )
              .append( " -> " )
              .append( partitionIdsByReplicaIndex.get( i ).size() )
              .append( " partitions, " );
        }

        sb.delete( sb.length() - 2, sb.length() );
        sb.append( "}" );

        return sb.toString();
    }

}
