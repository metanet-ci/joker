package cs.bilkent.joker.engine.kvstore.impl;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;
import javax.inject.Singleton;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import cs.bilkent.joker.engine.kvstore.KVStoreContext;
import cs.bilkent.joker.engine.kvstore.KVStoreContextManager;
import cs.bilkent.joker.engine.partition.PartitionService;
import cs.bilkent.joker.operator.kvstore.KVStore;
import cs.bilkent.joker.operator.kvstore.impl.InMemoryKVStore;
import cs.bilkent.joker.utils.Pair;

@Singleton
@NotThreadSafe
public class KVStoreContextManagerImpl implements KVStoreContextManager
{

    private static final Logger LOGGER = LoggerFactory.getLogger( KVStoreContextManagerImpl.class );


    private final PartitionService partitionService;

    private final Map<Pair<Integer, String>, DefaultKVStoreContext> defaultKVStoreContexts = new HashMap<>();

    private final Map<Pair<Integer, String>, PartitionedKVStoreContext[]> partitionedKvStoreContexts = new HashMap<>();

    private final Map<Pair<Integer, String>, KVStore[]> kvStores = new HashMap<>();

    @Inject
    public KVStoreContextManagerImpl ( final PartitionService partitionService )
    {
        this.partitionService = partitionService;
    }

    @Override
    public KVStoreContext createDefaultKVStoreContext ( final int regionId, final String operatorId )
    {
        checkArgument( regionId >= 0, "invalid regionId %s", regionId );
        checkArgument( operatorId != null, "null operatorId for regionId %s", regionId );

        return defaultKVStoreContexts.computeIfAbsent( Pair.of( regionId, operatorId ), p ->
        {
            checkState( !kvStores.containsKey( p ), "default kvStore for <regionId, operatorId> %s already exists!", p );
            final KVStore kvStore = new InMemoryKVStore();
            kvStores.put( p, new KVStore[] { kvStore } );
            return new DefaultKVStoreContext( operatorId, kvStore );
        } );
    }

    public KVStoreContext getDefaultKVStoreContext ( final int regionId, final String operatorId )
    {
        return defaultKVStoreContexts.get( Pair.of( regionId, operatorId ) );
    }

    @Override
    public KVStoreContext[] createPartitionedKVStoreContexts ( final int regionId, final int replicaCount, final String operatorId )
    {
        checkArgument( regionId >= 0, "invalid regionId %s", regionId );
        checkArgument( operatorId != null, "null operatorId for regionId %s", regionId );
        checkArgument( replicaCount > 0, "invalid replicaCount %s for regionId %s operatorId %s", replicaCount, regionId );

        return partitionedKvStoreContexts.computeIfAbsent( Pair.of( regionId, operatorId ), p ->
        {
            checkState( !kvStores.containsKey( p ), "partitioned kvStore for <regionId, operatorId> %s already exists!", p );
            final int partitionCount = partitionService.getPartitionCount();
            final KVStore[] k = new KVStore[ partitionCount ];
            for ( int i = 0; i < partitionCount; i++ )
            {
                k[ i ] = new InMemoryKVStore();
            }
            kvStores.put( p, k );
            final int[] partitions = partitionService.getOrCreatePartitionDistribution( regionId, replicaCount );
            final PartitionedKVStoreContext[] kvStoreContexts = new PartitionedKVStoreContext[ replicaCount ];
            for ( int replicaIndex = 0; replicaIndex < replicaCount; replicaIndex++ )
            {
                kvStoreContexts[ replicaIndex ] = new PartitionedKVStoreContext( operatorId, replicaIndex, k, partitions );
            }

            LOGGER.info( "kvStoreContext is created with {} kvStores for operator: {}", partitionCount, operatorId );

            return kvStoreContexts;
        } );
    }

    public KVStoreContext[] getPartitionedKVStoreContexts ( final int regionId, final String operatorId )
    {
        return partitionedKvStoreContexts.get( Pair.of( regionId, operatorId ) );
    }

    public KVStore[] getKVStores ( final int regionId, final String operatorId )
    {
        return kvStores.get( Pair.of( regionId, operatorId ) );
    }

    @Override
    public boolean releaseDefaultKVStoreContext ( final int regionId, final String operatorId )
    {
        final DefaultKVStoreContext kvStoreContext = defaultKVStoreContexts.remove( Pair.of( regionId, operatorId ) );

        if ( kvStoreContext != null )
        {
            releaseKVStores( regionId, operatorId );
            LOGGER.info( "default kv store of region {} operator {} is released", regionId, operatorId );
            return true;
        }

        LOGGER.error( "default kv store of region {} operator {} is not found for releasing", regionId, operatorId );
        return false;
    }

    @Override
    public boolean releasePartitionedKVStoreContext ( final int regionId, final String operatorId )
    {
        final PartitionedKVStoreContext[] kvStoreContexts = partitionedKvStoreContexts.remove( Pair.of( regionId, operatorId ) );

        if ( kvStoreContexts != null )
        {
            releaseKVStores( regionId, operatorId );
            LOGGER.info( "partitioned kv stores of region {} operator {} are released", regionId, operatorId );
            return true;
        }

        LOGGER.error( "partitioned kv stores of region {} operator {} are not found for releasing", regionId, operatorId );
        return false;
    }

    private void releaseKVStores ( final int regionId, final String operatorId )
    {
        final Pair<Integer, String> p = Pair.of( regionId, operatorId );
        final KVStore[] kvs = kvStores.remove( p );
        checkState( kvs != null, "kvStores not found for <regionId, operatorId> %s", p );
        for ( KVStore k : kvs )
        {
            k.clear();
        }
    }

}
