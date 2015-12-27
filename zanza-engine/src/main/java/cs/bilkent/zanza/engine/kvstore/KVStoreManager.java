package cs.bilkent.zanza.engine.kvstore;

import java.util.List;

import cs.bilkent.zanza.operator.spec.OperatorType;

public interface KVStoreManager
{

    KVStoreContext createKVStoreContext ( String operatorId,
                                          OperatorType operatorType, List<String> partitionFieldNames,
                                          int kvStoreInstanceCount );

    boolean releaseKVStoreContext ( String operatorId );

}
