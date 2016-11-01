package cs.bilkent.joker.engine.kvstore.impl;

import java.util.Arrays;
import java.util.HashMap;
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
import cs.bilkent.joker.operator.kvstore.KVStore;
import cs.bilkent.joker.operator.kvstore.impl.InMemoryKVStore;
import cs.bilkent.joker.utils.Pair;

@Singleton
@NotThreadSafe
public class OperatorKVStoreManagerImpl implements OperatorKVStoreManager
{

    private static final Logger LOGGER = LoggerFactory.getLogger( OperatorKVStoreManagerImpl.class );


    private final Map<Pair<Integer, String>, DefaultOperatorKVStore> defaultOperatorKVStores = new HashMap<>();

    private final Map<Pair<Integer, String>, PartitionedOperatorKVStore[]> partitionedOperatorKvStores = new HashMap<>();

    private final Map<Pair<Integer, String>, KVStoreContainer[]> kvStoreContainersByOperatorId = new HashMap<>();

    @Override
    public OperatorKVStore createDefaultOperatorKVStore ( final int regionId, final String operatorId )
    {
        checkArgument( regionId >= 0, "invalid regionId %s", regionId );
        checkArgument( operatorId != null, "null operatorId for regionId %s", regionId );

        final Pair<Integer, String> key = Pair.of( regionId, operatorId );
        checkState( !defaultOperatorKVStores.containsKey( key ),
                    "default operator tuple queue already exists for regionId=%s operatorId=%s",
                    regionId,
                    operatorId );

        final DefaultOperatorKVStore operatorKVStore = new DefaultOperatorKVStore( operatorId, new InMemoryKVStore() );
        defaultOperatorKVStores.put( key, operatorKVStore );
        LOGGER.info( "default operator kv store is created for operator: {}", operatorId );

        return operatorKVStore;
    }

    public OperatorKVStore getDefaultOperatorKVStore ( final int regionId, final String operatorId )
    {
        return defaultOperatorKVStores.get( Pair.of( regionId, operatorId ) );
    }

    @Override
    public OperatorKVStore[] createPartitionedOperatorKVStore ( final int regionId,
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
        checkState( !partitionedOperatorKvStores.containsKey( key ),
                    "partitioned operator tuple queue already exists for regionId=%s operatorId=%s",
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

        partitionedOperatorKvStores.put( key, operatorKVStores );
        LOGGER.info( "operator kv store is created with {} kvStores for operator: {}", partitionCount, operatorId );

        return operatorKVStores;
    }

    public OperatorKVStore[] getPartitionedOperatorKVStore ( final int regionId, final String operatorId )
    {
        return partitionedOperatorKvStores.get( Pair.of( regionId, operatorId ) );
    }

    public KVStoreContainer[] getKVStoreContainers ( final int regionId, final String operatorId )
    {
        final KVStoreContainer[] containers = kvStoreContainersByOperatorId.get( Pair.of( regionId, operatorId ) );
        return containers != null ? Arrays.copyOf( containers, containers.length ) : null;
    }

    @Override
    public void releaseDefaultOperatorKVStore ( final int regionId, final String operatorId )
    {
        final DefaultOperatorKVStore operatorKVStore = defaultOperatorKVStores.remove( Pair.of( regionId, operatorId ) );
        checkState( operatorKVStore != null,
                    "default kv store of regionId=%s operatorId={} is not found for releasing",
                    regionId,
                    operatorId );

        final KVStore kvStore = operatorKVStore.getKVStore( null );
        kvStore.clear();
        LOGGER.info( "default kv store of region {} operator {} is released", regionId, operatorId );
    }

    @Override
    public void releasePartitionedOperatorKVStore ( final int regionId, final String operatorId )
    {
        final PartitionedOperatorKVStore[] operatorKVStores = partitionedOperatorKvStores.remove( Pair.of( regionId, operatorId ) );
        checkState( operatorKVStores != null,
                    "partitioned kv stores of regionId=%s operatorId=%s are not found for releasing",
                    regionId,
                    operatorId );

        releaseKVStoreContainers( regionId, operatorId );
        LOGGER.info( "partitioned kv stores of region {} operator {} are released", regionId, operatorId );
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
