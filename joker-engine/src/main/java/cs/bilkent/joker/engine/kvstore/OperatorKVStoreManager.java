package cs.bilkent.joker.engine.kvstore;

import cs.bilkent.joker.engine.partition.PartitionDistribution;

public interface OperatorKVStoreManager
{

    OperatorKVStore createDefaultOperatorKVStore ( int regionId, String operatorId );

    OperatorKVStore[] createPartitionedOperatorKVStores ( int regionId, String operatorId, PartitionDistribution partitionDistribution );

    OperatorKVStore[] rebalancePartitionedOperatorKVStores ( int regionId,
                                                             String operatorId,
                                                             PartitionDistribution currentPartitionDistribution,
                                                             PartitionDistribution newPartitionDistribution );

    void releaseDefaultOperatorKVStore ( int regionId, String operatorId );

    void releasePartitionedOperatorKVStores ( int regionId, String operatorId );

}
