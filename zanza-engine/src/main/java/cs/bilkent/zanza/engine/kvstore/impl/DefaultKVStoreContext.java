package cs.bilkent.zanza.engine.kvstore.impl;

import cs.bilkent.zanza.engine.kvstore.KVStoreContext;
import cs.bilkent.zanza.operator.kvstore.KVStore;

public class DefaultKVStoreContext implements KVStoreContext
{

    private final String operatorId;

    private final KVStore kvStore;

    DefaultKVStoreContext ( final String operatorId, final KVStore kvStore )
    {
        this.operatorId = operatorId;
        this.kvStore = kvStore;
    }

    @Override
    public String getOperatorId ()
    {
        return operatorId;
    }

    @Override
    public KVStore getKVStore ( final Object key )
    {
        return kvStore;
    }

}
