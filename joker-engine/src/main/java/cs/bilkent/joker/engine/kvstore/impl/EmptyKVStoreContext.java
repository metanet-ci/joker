package cs.bilkent.joker.engine.kvstore.impl;

import cs.bilkent.joker.engine.kvstore.KVStoreContext;
import cs.bilkent.joker.engine.partition.PartitionKey;
import cs.bilkent.joker.operator.kvstore.KVStore;

public class EmptyKVStoreContext implements KVStoreContext
{

    private final String operatorId;

    public EmptyKVStoreContext ( final String operatorId )
    {
        this.operatorId = operatorId;
    }

    @Override
    public String getOperatorId ()
    {
        return operatorId;
    }

    @Override
    public KVStore getKVStore ( final PartitionKey key )
    {
        return null;
    }

}
