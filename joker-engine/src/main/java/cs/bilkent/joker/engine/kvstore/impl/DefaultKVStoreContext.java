package cs.bilkent.joker.engine.kvstore.impl;

import cs.bilkent.joker.engine.kvstore.KVStoreContext;
import cs.bilkent.joker.engine.partition.PartitionKey;
import cs.bilkent.joker.operator.kvstore.KVStore;

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
    public KVStore getKVStore ( final PartitionKey key )
    {
        return kvStore;
    }

}
