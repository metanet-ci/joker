package cs.bilkent.zanza.engine.kvstore.impl;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.zanza.engine.kvstore.KVStoreContext;
import cs.bilkent.zanza.engine.kvstore.KVStoreManager;
import cs.bilkent.zanza.operator.kvstore.KVStore;
import cs.bilkent.zanza.operator.kvstore.impl.InMemoryKVStore;

public class KVStoreManagerImpl implements KVStoreManager
{

    private static final Logger LOGGER = LoggerFactory.getLogger( KVStoreManagerImpl.class );


    private final ConcurrentMap<String, KVStoreContextImpl> kvStoresByOperatorId = new ConcurrentHashMap<>();

    @Override
    public KVStoreContext createKVStoreContext ( final String operatorId, final int replicaCount )
    {
        checkArgument( operatorId != null );
        checkArgument( replicaCount > 0 );

        return kvStoresByOperatorId.computeIfAbsent( operatorId, opId -> {
            final KVStore[] kvStores = new KVStore[ replicaCount ];
            for ( int i = 0; i < replicaCount; i++ )
            {
                kvStores[ i ] = new InMemoryKVStore();
            }
            LOGGER.info( "kvStoreContext is created with {} kvStores for operator: {}", operatorId, operatorId );

            return new KVStoreContextImpl( operatorId, kvStores );
        } );
    }

    @Override
    public boolean releaseKVStoreContext ( final String operatorId )
    {
        final KVStoreContextImpl kvStoreContext = kvStoresByOperatorId.remove( operatorId );
        boolean removed = kvStoreContext != null;

        if ( removed )
        {
            kvStoreContext.clear();
            LOGGER.info( "kvStoreContext of operator: {} is released", operatorId );
        }
        else
        {
            LOGGER.warn( "kvStoreContext of operator: {} is not found for releasing", operatorId );
        }

        return removed;
    }

}
