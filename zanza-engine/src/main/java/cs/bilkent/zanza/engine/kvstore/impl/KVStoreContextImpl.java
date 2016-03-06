package cs.bilkent.zanza.engine.kvstore.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cs.bilkent.zanza.engine.kvstore.KVStoreContext;
import cs.bilkent.zanza.operator.kvstore.KVStore;

public class KVStoreContextImpl implements KVStoreContext
{

    private static final Logger LOGGER = LoggerFactory.getLogger( KVStoreContextImpl.class );


    private final String operatorId;

    private final KVStore[] kvStores;

    public KVStoreContextImpl ( final String operatorId, final KVStore[] kvStores )
    {
        this.operatorId = operatorId;
        this.kvStores = kvStores;
    }

    @Override
    public String getOperatorId ()
    {
        return operatorId;
    }

    @Override
    public int getKVStoreCount ()
    {
        return kvStores.length;
    }

    @Override
    public KVStore getKVStore ( final int replicaIndex )
    {
        return kvStores[ replicaIndex ];
    }

    public void clear ()
    {
        LOGGER.info( "Clearing kv stores of operatorId: {}", operatorId );

        for ( KVStore kvStore : kvStores )
        {
            kvStore.clear();
        }
    }

}
