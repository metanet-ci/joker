package cs.bilkent.joker.engine.kvstore.impl;

import cs.bilkent.joker.engine.kvstore.OperatorKVStore;
import cs.bilkent.joker.engine.partition.PartitionKey;
import cs.bilkent.joker.operator.kvstore.KVStore;

public class DefaultOperatorKVStore implements OperatorKVStore
{

    private final String operatorId;

    private final KVStore kvStore;

    DefaultOperatorKVStore ( final String operatorId, final KVStore kvStore )
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
