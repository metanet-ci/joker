package cs.bilkent.joker.engine.kvstore;

import cs.bilkent.joker.engine.partition.PartitionDistribution;

public interface OperatorKVStoreManager
{

    OperatorKVStore createDefaultOperatorKVStore ( int regionId, String operatorId );

    OperatorKVStore[] createPartitionedOperatorKVStore ( int regionId, String operatorId, PartitionDistribution partitionDistribution );

    void releaseDefaultOperatorKVStore ( int regionId, String operatorId );

    void releasePartitionedOperatorKVStore ( int regionId, String operatorId );

}
