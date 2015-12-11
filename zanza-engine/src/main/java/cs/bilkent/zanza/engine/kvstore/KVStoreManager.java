package cs.bilkent.zanza.engine.kvstore;

import cs.bilkent.zanza.operator.OperatorType;
import cs.bilkent.zanza.operator.PartitionKeyExtractor;

public interface KVStoreManager
{

    KVStoreContext createKVStoreContext ( String operatorId,
                                          OperatorType operatorType,
                                          PartitionKeyExtractor partitionKeyExtractor,
                                          int kvStoreInstanceCount );

    boolean releaseKVStoreContext ( String operatorId );

}
