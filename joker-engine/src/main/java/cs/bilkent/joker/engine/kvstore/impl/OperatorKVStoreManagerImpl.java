package cs.bilkent.joker.engine.kvstore.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Singleton;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import cs.bilkent.joker.engine.kvstore.OperatorKVStore;
import cs.bilkent.joker.engine.kvstore.OperatorKVStoreManager;
import cs.bilkent.joker.engine.partition.PartitionDistribution;
import cs.bilkent.joker.operator.impl.InMemoryKVStore;
import cs.bilkent.joker.operator.kvstore.KVStore;
import cs.bilkent.joker.utils.Pair;
import static java.util.Arrays.copyOf;

@Singleton
@NotThreadSafe
public class OperatorKVStoreManagerImpl implements OperatorKVStoreManager
{

    private static final Logger LOGGER = LoggerFactory.getLogger( OperatorKVStoreManagerImpl.class );


    private final Map<Pair<Integer, String>, DefaultOperatorKVStore> defaultOperatorKVStores = new HashMap<>();

    private final Map<Pair<Integer, String>, PartitionedOperatorKVStore[]> partitionedOperatorKVStores = new HashMap<>();

    private final Map<Pair<Integer, String>, KVStoreContainer[]> kvStoreContainersByOperatorId = new HashMap<>();

    @Override
    public OperatorKVStore createDefaultKVStore ( final int regionId, final String operatorId )
    {
        checkArgument( regionId >= 0, "invalid regionId %s", regionId );
        checkArgument( operatorId != null, "null operatorId for regionId %s", regionId );

        final Pair<Integer, String> key = Pair.of( regionId, operatorId );
        checkState( !defaultOperatorKVStores.containsKey( key ), "default kv store already exists for regionId=%s operatorId=%s",
                    regionId,
                    operatorId );

        final DefaultOperatorKVStore operatorKVStore = new DefaultOperatorKVStore( operatorId, new InMemoryKVStore() );
        defaultOperatorKVStores.put( key, operatorKVStore );
        LOGGER.debug( "default kv store is created for operator: {}", operatorId );

        return operatorKVStore;
    }

    @Override
    public OperatorKVStore getDefaultKVStore ( final int regionId, final String operatorId )
    {
        return defaultOperatorKVStores.get( Pair.of( regionId, operatorId ) );
    }

    @Override
    public OperatorKVStore[] createPartitionedKVStores ( final int regionId,
                                                         final String operatorId,
                                                         final PartitionDistribution partitionDistribution )
    {
        checkArgument( regionId >= 0, "invalid regionId %s", regionId );
        checkArgument( operatorId != null, "null operatorId for regionId %s", regionId );
        checkArgument( partitionDistribution.getReplicaCount() > 0,
                       "invalid replicaCount %s for regionId %s operatorId %s",
                       partitionDistribution.getReplicaCount(),
                       regionId );

        final Pair<Integer, String> key = Pair.of( regionId, operatorId );
        checkState( !partitionedOperatorKVStores.containsKey( key ), "partitioned kv stores already exists for regionId=%s operatorId=%s",
                    regionId,
                    operatorId );

        final int partitionCount = partitionDistribution.getPartitionCount();
        final KVStoreContainer[] containers = new KVStoreContainer[ partitionCount ];
        for ( int i = 0; i < partitionCount; i++ )
        {
            containers[ i ] = new KVStoreContainer( i );
        }
        kvStoreContainersByOperatorId.put( key, containers );

        final int[] partitions = partitionDistribution.getDistribution();
        final PartitionedOperatorKVStore[] operatorKVStores = new PartitionedOperatorKVStore[ partitionDistribution.getReplicaCount() ];
        for ( int replicaIndex = 0; replicaIndex < partitionDistribution.getReplicaCount(); replicaIndex++ )
        {
            operatorKVStores[ replicaIndex ] = new PartitionedOperatorKVStore( operatorId, replicaIndex, containers, partitions );
        }

        partitionedOperatorKVStores.put( key, operatorKVStores );
        LOGGER.debug( "operator kv store is created with {} kvStores for operator: {}", partitionCount, operatorId );

        return operatorKVStores;
    }

    @Override
    public OperatorKVStore[] rebalancePartitionedKVStores ( final int regionId,
                                                            final String operatorId,
                                                            final PartitionDistribution currentPartitionDistribution,
                                                            final PartitionDistribution newPartitionDistribution )
    {
        final Pair<Integer, String> key = Pair.of( regionId, operatorId );
        PartitionedOperatorKVStore[] kvStores = partitionedOperatorKVStores.get( key );
        checkState( kvStores != null, "partitioned kv stores do not exist for regionId=%s operatorId=%s", regionId, operatorId );

        final Map<Integer, KVStoreContainer> migratingPartitions = getMigratingPartitions( currentPartitionDistribution,
                                                                                           newPartitionDistribution,
                                                                                           kvStores );

        kvStores = migratePartitions( currentPartitionDistribution, newPartitionDistribution, kvStores, migratingPartitions );

        partitionedOperatorKVStores.put( key, kvStores );
        LOGGER.debug( "partitioned operator kv stores of regionId={} operatorId={} are rebalanced to {} replicas",
                      regionId,
                      operatorId,
                      newPartitionDistribution.getReplicaCount() );

        return kvStores;
    }

    private Map<Integer, KVStoreContainer> getMigratingPartitions ( final PartitionDistribution currentPartitionDistribution,
                                                                    final PartitionDistribution newPartitionDistribution,
                                                                    final PartitionedOperatorKVStore[] queues )
    {
        final Map<Integer, KVStoreContainer> migratingPartitions = new HashMap<>();

        for ( int replicaIndex = 0; replicaIndex < queues.length; replicaIndex++ )
        {
            final List<Integer> partitionIds = currentPartitionDistribution.getPartitionIdsMigratedFromReplicaIndex(
                    newPartitionDistribution,
                    replicaIndex );
            if ( partitionIds.size() > 0 )
            {
                queues[ replicaIndex ].releasePartitions( partitionIds ).forEach( c -> migratingPartitions.put( c.getPartitionId(), c ) );
            }
        }

        return migratingPartitions;
    }

    private PartitionedOperatorKVStore[] migratePartitions ( final PartitionDistribution currentPartitionDistribution,
                                                             final PartitionDistribution newPartitionDistribution,
                                                             final PartitionedOperatorKVStore[] kvStores,
                                                             final Map<Integer, KVStoreContainer> movingPartitions )
    {
        final String operatorId = kvStores[ 0 ].getOperatorId();
        final int currentReplicaCount = currentPartitionDistribution.getReplicaCount();
        final int newReplicaCount = newPartitionDistribution.getReplicaCount();
        final PartitionedOperatorKVStore[] newKVStores = copyOf( kvStores, newReplicaCount );
        if ( currentReplicaCount > newReplicaCount )
        {
            for ( int replicaIndex = 0; replicaIndex < newReplicaCount; replicaIndex++ )
            {
                final List<KVStoreContainer> partitions = getPartitionsMigratedToReplicaIndex( currentPartitionDistribution,
                                                                                               newPartitionDistribution,
                                                                                               movingPartitions,
                                                                                               replicaIndex );
                newKVStores[ replicaIndex ].acquirePartitions( partitions );
            }
        }
        else
        {
            for ( int replicaIndex = currentReplicaCount; replicaIndex < newReplicaCount; replicaIndex++ )
            {
                final int partitionCount = newPartitionDistribution.getPartitionCount();
                final KVStoreContainer containers[] = new KVStoreContainer[ partitionCount ];
                getPartitionsMigratedToReplicaIndex( currentPartitionDistribution,
                                                     newPartitionDistribution,
                                                     movingPartitions,
                                                     replicaIndex ).forEach( p -> containers[ p.getPartitionId() ] = p );
                newKVStores[ replicaIndex ] = new PartitionedOperatorKVStore( operatorId,
                                                                              replicaIndex,
                                                                              containers,
                                                                              newPartitionDistribution.getDistribution() );

            }
        }

        return newKVStores;
    }

    private List<KVStoreContainer> getPartitionsMigratedToReplicaIndex ( final PartitionDistribution currentPartitionDistribution,
                                                                         final PartitionDistribution newPartitionDistribution,
                                                                         final Map<Integer, KVStoreContainer> movingPartitions,
                                                                         final int replicaIndex )
    {
        final List<Integer> partitionIds = currentPartitionDistribution.getPartitionIdsMigratedToReplicaIndex( newPartitionDistribution,
                                                                                                               replicaIndex );

        final List<KVStoreContainer> partitions = new ArrayList<>();
        for ( Integer partitionId : partitionIds )
        {
            partitions.add( movingPartitions.remove( partitionId ) );
        }

        return partitions;
    }


    @Override
    public OperatorKVStore[] getPartitionedKVStores ( final int regionId, final String operatorId )
    {
        final Pair<Integer, String> key = Pair.of( regionId, operatorId );
        final PartitionedOperatorKVStore[] operatorKVStores = partitionedOperatorKVStores.get( key );
        return operatorKVStores != null ? copyOf( operatorKVStores, operatorKVStores.length ) : null;
    }

    public KVStoreContainer[] getKVStoreContainers ( final int regionId, final String operatorId )
    {
        final KVStoreContainer[] containers = kvStoreContainersByOperatorId.get( Pair.of( regionId, operatorId ) );
        return containers != null ? copyOf( containers, containers.length ) : null;
    }

    @Override
    public void releaseDefaultKVStore ( final int regionId, final String operatorId )
    {
        final DefaultOperatorKVStore operatorKVStore = defaultOperatorKVStores.remove( Pair.of( regionId, operatorId ) );
        checkState( operatorKVStore != null,
                    "default kv store of regionId=%s operatorId={} is not found for releasing",
                    regionId,
                    operatorId );

        final KVStore kvStore = operatorKVStore.getKVStore( null );
        kvStore.clear();
        LOGGER.debug( "default kv store of region {} operator {} is released", regionId, operatorId );
    }

    @Override
    public void releasePartitionedKVStores ( final int regionId, final String operatorId )
    {
        final PartitionedOperatorKVStore[] operatorKVStores = partitionedOperatorKVStores.remove( Pair.of( regionId, operatorId ) );
        checkState( operatorKVStores != null,
                    "partitioned kv stores of regionId=%s operatorId=%s are not found for releasing",
                    regionId,
                    operatorId );

        releaseKVStoreContainers( regionId, operatorId );
        LOGGER.debug( "partitioned kv stores of region {} operator {} are released", regionId, operatorId );
    }

    private void releaseKVStoreContainers ( final int regionId, final String operatorId )
    {
        final Pair<Integer, String> p = Pair.of( regionId, operatorId );
        final KVStoreContainer[] containers = kvStoreContainersByOperatorId.remove( p );
        checkState( containers != null, "kvStores not found for <regionId, operatorId> %s", p );
        for ( KVStoreContainer container : containers )
        {
            container.clear();
        }
    }

}
