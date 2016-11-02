package cs.bilkent.joker.engine.kvstore.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static cs.bilkent.joker.com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.joker.engine.kvstore.OperatorKVStore;
import cs.bilkent.joker.engine.partition.PartitionKey;
import static cs.bilkent.joker.engine.partition.PartitionUtil.getPartitionId;
import cs.bilkent.joker.operator.kvstore.KVStore;

public class PartitionedOperatorKVStore implements OperatorKVStore
{

    private static final Logger LOGGER = LoggerFactory.getLogger( PartitionedOperatorKVStore.class );


    private final String operatorId;

    private final int replicaIndex;

    private final KVStoreContainer[] kvStoreContainers;

    private final int partitionCount;

    PartitionedOperatorKVStore ( final String operatorId,
                                 final int replicaIndex,
                                 final KVStoreContainer[] kvStoreContainers,
                                 final int[] partitions )
    {
        this.operatorId = operatorId;
        this.replicaIndex = replicaIndex;
        this.kvStoreContainers = Arrays.copyOf( kvStoreContainers, kvStoreContainers.length );
        this.partitionCount = partitions.length;
        for ( int partitionId = 0; partitionId < partitionCount; partitionId++ )
        {
            if ( partitions[ partitionId ] != replicaIndex )
            {
                this.kvStoreContainers[ partitionId ] = null;
            }
        }
    }

    @Override
    public String getOperatorId ()
    {
        return operatorId;
    }

    @Override
    public KVStore getKVStore ( final PartitionKey key )
    {
        if ( key == null )
        {
            return null;
        }

        final int partitionHash = key.partitionHashCode();
        final int partitionId = getPartitionId( partitionHash, partitionCount );
        final KVStoreContainer container = kvStoreContainers[ partitionId ];
        return container.getOrCreateKVStore( key );
    }

    public void acquirePartitions ( final List<KVStoreContainer> partitions )
    {
        checkArgument( partitions != null, "cannot acquire null partitions in operator kvStore of operatorId=%s replicaIndex=%s",
                       operatorId,
                       replicaIndex );
        for ( KVStoreContainer partition : partitions )
        {
            checkArgument( kvStoreContainers[ partition.getPartitionId() ] == null,
                           "partitionId=%s is already acquired by operatorId=%s replicaIndex=%s",
                           partition.getPartitionId(),
                           operatorId,
                           replicaIndex );
        }

        for ( KVStoreContainer partition : partitions )
        {
            kvStoreContainers[ partition.getPartitionId() ] = partition;
        }

        final int[] partitionIds = partitions.stream().mapToInt( KVStoreContainer::getPartitionId ).toArray();
        LOGGER.info( "partitions={} are acquired by operatorId={} replicaIndex={}", partitionIds, operatorId, replicaIndex );
    }

    public List<KVStoreContainer> releasePartitions ( final List<Integer> partitionIds )
    {
        checkArgument( partitionIds != null, "cannot release null partition ids of operatorId=%s replicaIndex=%s",
                       operatorId,
                       replicaIndex );

        for ( int partitionId : partitionIds )
        {
            checkArgument( kvStoreContainers[ partitionId ] != null,
                           "partitionId=%s is not owned by operatorId=%s replicaIndex=%s",
                           partitionId,
                           operatorId,
                           replicaIndex );
        }

        final List<KVStoreContainer> left = new ArrayList<>();
        for ( int partitionId : partitionIds )
        {
            left.add( kvStoreContainers[ partitionId ] );
            kvStoreContainers[ partitionId ] = null;
        }

        LOGGER.info( "partitions={} are left by operatorId={} replicaIndex={}", partitionIds, operatorId, replicaIndex );

        return left;
    }

}
