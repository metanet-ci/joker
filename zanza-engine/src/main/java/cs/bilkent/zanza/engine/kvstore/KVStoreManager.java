package cs.bilkent.zanza.engine.kvstore;

import cs.bilkent.zanza.operator.PartitionKeyExtractor;
import cs.bilkent.zanza.operator.spec.OperatorType;

public interface KVStoreManager
{

    KVStoreContext createKVStoreContext ( String operatorId,
                                          OperatorType operatorType,
                                          PartitionKeyExtractor partitionKeyExtractor,
                                          int kvStoreInstanceCount );

    boolean releaseKVStoreContext ( String operatorId );

}
