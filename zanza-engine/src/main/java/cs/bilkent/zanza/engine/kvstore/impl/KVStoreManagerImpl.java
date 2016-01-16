package cs.bilkent.zanza.engine.kvstore.impl;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.zanza.engine.kvstore.KVStoreContext;
import cs.bilkent.zanza.engine.kvstore.KVStoreManager;
import static cs.bilkent.zanza.engine.util.Preconditions.checkOperatorTypeAndPartitionKeyFieldNames;
import cs.bilkent.zanza.kvstore.InMemoryKVStore;
import cs.bilkent.zanza.kvstore.KVStore;
import cs.bilkent.zanza.operator.spec.OperatorType;
import static cs.bilkent.zanza.operator.spec.OperatorType.PARTITIONED_STATEFUL;
import static cs.bilkent.zanza.operator.spec.OperatorType.STATELESS;

public class KVStoreManagerImpl implements KVStoreManager
{

    private static final Logger LOGGER = LoggerFactory.getLogger( KVStoreManagerImpl.class );


    private final ConcurrentMap<String, KVStoreContextImpl> kvStoresByOperatorId = new ConcurrentHashMap<>();

    @Override
    public KVStoreContext createKVStoreContext ( final String operatorId,
                                                 final OperatorType operatorType,
                                                 final List<String> partitionFieldNames,
                                                 final int kvStoreInstanceCount )
    {
        checkArgument( operatorId != null );
        checkArgument( operatorType != null );
        checkArgument( operatorType != STATELESS );
        checkArgument( kvStoreInstanceCount > 0 );
        checkOperatorTypeAndPartitionKeyFieldNames( operatorType, partitionFieldNames );
        checkArgument( operatorType == PARTITIONED_STATEFUL || kvStoreInstanceCount == 1 );

        return kvStoresByOperatorId.computeIfAbsent( operatorId, opId -> {
            final KVStore[] kvStores = new KVStore[ kvStoreInstanceCount ];
            for ( int i = 0; i < kvStoreInstanceCount; i++ )
            {
                kvStores[ i ] = new InMemoryKVStore();
            }
            LOGGER.info( "kvStoreContext is created with {} kvStores for operator: {}", kvStoreInstanceCount, operatorId );

            return new KVStoreContextImpl( operatorId, operatorType, kvStores );
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
