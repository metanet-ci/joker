package cs.bilkent.zanza.engine.kvstore.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkState;
import cs.bilkent.zanza.engine.kvstore.KVStoreContext;
import cs.bilkent.zanza.kvstore.KVStore;
import cs.bilkent.zanza.kvstore.KeyPrefixedInMemoryKvStore;
import cs.bilkent.zanza.operator.OperatorType;

public class KVStoreContextImpl implements KVStoreContext
{

    private static final Logger LOGGER = LoggerFactory.getLogger( KVStoreContextImpl.class );


    private final String operatorId;

    private final OperatorType operatorType;

    private final KVStore[] kvStores;

    public KVStoreContextImpl ( final String operatorId, final OperatorType operatorType, final KVStore[] kvStores )
    {
        this.operatorId = operatorId;
        this.operatorType = operatorType;
        this.kvStores = kvStores;
    }

    @Override
    public String getOperatorId ()
    {
        return operatorId;
    }

    @Override
    public KVStore getKVStore ()
    {
        checkState( operatorType != OperatorType.PARTITIONED_STATEFUL );
        return kvStores[ 0 ];
    }

    @Override
    public KVStore getKVStore ( final Object partitionKey )
    {
        checkState( operatorType == OperatorType.PARTITIONED_STATEFUL );

        final int i = partitionKey.hashCode() % kvStores.length;
        final KVStore kvStore = kvStores[ i ];

        return new KeyPrefixedInMemoryKvStore( partitionKey, kvStore );
    }

    public void clear ()
    {
        LOGGER.info( "Clearing kv stores of operatorId: {}", operatorId );

        for ( KVStore kvStore : kvStores )
        {
            kvStore.clear();
        }
    }

    public int getKVStoresSize ()
    {
        return kvStores.length;
    }

}
