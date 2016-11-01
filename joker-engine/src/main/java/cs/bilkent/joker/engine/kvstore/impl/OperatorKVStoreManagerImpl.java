package cs.bilkent.joker.engine.kvstore.impl;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;
import javax.inject.Singleton;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import cs.bilkent.joker.engine.kvstore.OperatorKVStore;
import cs.bilkent.joker.engine.kvstore.OperatorKVStoreManager;
import cs.bilkent.joker.engine.partition.PartitionService;
import cs.bilkent.joker.operator.kvstore.KVStore;
import cs.bilkent.joker.operator.kvstore.impl.InMemoryKVStore;
import cs.bilkent.joker.utils.Pair;

@Singleton
@NotThreadSafe
public class OperatorKVStoreManagerImpl implements OperatorKVStoreManager
{

    private static final Logger LOGGER = LoggerFactory.getLogger( OperatorKVStoreManagerImpl.class );


    private final PartitionService partitionService;

    private final Map<Pair<Integer, String>, DefaultOperatorKVStore> defaultOperatorKVStores = new HashMap<>();

    private final Map<Pair<Integer, String>, PartitionedOperatorKVStore[]> partitionedOperatorKvStores = new HashMap<>();

    private final Map<Pair<Integer, String>, KVStoreContainer[]> kvStoreContainers = new HashMap<>();

    @Inject
    public OperatorKVStoreManagerImpl ( final PartitionService partitionService )
    {
        this.partitionService = partitionService;
    }

    @Override
    public OperatorKVStore createDefaultOperatorKVStore ( final int regionId, final String operatorId )
    {
        checkArgument( regionId >= 0, "invalid regionId %s", regionId );
        checkArgument( operatorId != null, "null operatorId for regionId %s", regionId );

        return defaultOperatorKVStores.computeIfAbsent( Pair.of( regionId, operatorId ), p ->
        {
            final KVStore kvStore = new InMemoryKVStore();
            return new DefaultOperatorKVStore( operatorId, kvStore );
        } );
    }

    public OperatorKVStore getDefaultOperatorKVStore ( final int regionId, final String operatorId )
    {
        return defaultOperatorKVStores.get( Pair.of( regionId, operatorId ) );
    }

    @Override
    public OperatorKVStore[] createPartitionedOperatorKVStore ( final int regionId, final int replicaCount, final String operatorId )
    {
        checkArgument( regionId >= 0, "invalid regionId %s", regionId );
        checkArgument( operatorId != null, "null operatorId for regionId %s", regionId );
        checkArgument( replicaCount > 0, "invalid replicaCount %s for regionId %s operatorId %s", replicaCount, regionId );

        return partitionedOperatorKvStores.computeIfAbsent( Pair.of( regionId, operatorId ), p ->
        {
            final int partitionCount = partitionService.getPartitionCount();
            final KVStoreContainer[] containers = new KVStoreContainer[ partitionCount ];
            for ( int i = 0; i < partitionCount; i++ )
            {
                containers[ i ] = new KVStoreContainer( i );
            }
            kvStoreContainers.put( p, containers );
            final int[] partitions = partitionService.getOrCreatePartitionDistribution( regionId, replicaCount );
            final PartitionedOperatorKVStore[] operatorKVStores = new PartitionedOperatorKVStore[ replicaCount ];
            for ( int replicaIndex = 0; replicaIndex < replicaCount; replicaIndex++ )
            {
                operatorKVStores[ replicaIndex ] = new PartitionedOperatorKVStore( operatorId, replicaIndex, containers, partitions );
            }

            LOGGER.info( "operator kv store is created with {} kvStores for operator: {}", partitionCount, operatorId );

            return operatorKVStores;
        } );
    }

    public OperatorKVStore[] getPartitionedOperatorKVStore ( final int regionId, final String operatorId )
    {
        return partitionedOperatorKvStores.get( Pair.of( regionId, operatorId ) );
    }

    public KVStoreContainer[] getKVStoreContainers ( final int regionId, final String operatorId )
    {
        final KVStoreContainer[] containers = kvStoreContainers.get( Pair.of( regionId, operatorId ) );
        return containers != null ? Arrays.copyOf( containers, containers.length ) : null;
    }

    @Override
    public boolean releaseDefaultOperatorKVStore ( final int regionId, final String operatorId )
    {
        final DefaultOperatorKVStore operatorKVStore = defaultOperatorKVStores.remove( Pair.of( regionId, operatorId ) );

        if ( operatorKVStore != null )
        {
            final KVStore kvStore = operatorKVStore.getKVStore( null );
            kvStore.clear();
            LOGGER.info( "default kv store of region {} operator {} is released", regionId, operatorId );
            return true;
        }

        LOGGER.error( "default kv store of region {} operator {} is not found for releasing", regionId, operatorId );
        return false;
    }

    @Override
    public boolean releasePartitionedOperatorKVStore ( final int regionId, final String operatorId )
    {
        final PartitionedOperatorKVStore[] operatorKVStores = partitionedOperatorKvStores.remove( Pair.of( regionId, operatorId ) );

        if ( operatorKVStores != null )
        {
            releaseKVStoreContainers( regionId, operatorId );
            LOGGER.info( "partitioned kv stores of region {} operator {} are released", regionId, operatorId );
            return true;
        }

        LOGGER.error( "partitioned kv stores of region {} operator {} are not found for releasing", regionId, operatorId );
        return false;
    }

    private void releaseKVStoreContainers ( final int regionId, final String operatorId )
    {
        final Pair<Integer, String> p = Pair.of( regionId, operatorId );
        final KVStoreContainer[] containers = kvStoreContainers.remove( p );
        checkState( containers != null, "kvStores not found for <regionId, operatorId> %s", p );
        for ( KVStoreContainer container : containers )
        {
            container.clear();
        }
    }

}
