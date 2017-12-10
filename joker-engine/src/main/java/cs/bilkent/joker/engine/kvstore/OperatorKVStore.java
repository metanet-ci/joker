package cs.bilkent.joker.engine.kvstore;

import cs.bilkent.joker.operator.kvstore.KVStore;
import cs.bilkent.joker.partition.impl.PartitionKey;


public interface OperatorKVStore
{

    String getOperatorId ();

    KVStore getKVStore ( PartitionKey key );

}
