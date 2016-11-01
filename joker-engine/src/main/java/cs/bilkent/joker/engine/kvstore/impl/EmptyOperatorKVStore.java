package cs.bilkent.joker.engine.kvstore.impl;

import cs.bilkent.joker.engine.kvstore.OperatorKVStore;
import cs.bilkent.joker.engine.partition.PartitionKey;
import cs.bilkent.joker.operator.kvstore.KVStore;

public class EmptyOperatorKVStore implements OperatorKVStore
{

    private final String operatorId;

    public EmptyOperatorKVStore ( final String operatorId )
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
