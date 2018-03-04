package cs.bilkent.joker.engine.kvstore;

import cs.bilkent.joker.engine.partition.PartitionDistribution;

public interface OperatorKVStoreManager
{

    OperatorKVStore createDefaultKVStore ( int regionId, String operatorId );

    OperatorKVStore getDefaultKVStore ( int regionId, String operatorId );

    OperatorKVStore[] createPartitionedKVStores ( int regionId, String operatorId, PartitionDistribution partitionDistribution );

    OperatorKVStore[] rebalancePartitionedKVStores ( int regionId,
                                                     String operatorId,
                                                     PartitionDistribution currentPartitionDistribution,
                                                     PartitionDistribution newPartitionDistribution );

    OperatorKVStore[] getPartitionedKVStores ( final int regionId, final String operatorId );

    default OperatorKVStore getPartitionedKVStore ( final int regionId, final String operatorId, final int replicaIndex )
    {
        return getPartitionedKVStoresOrFail( regionId, operatorId )[ replicaIndex ];
    }

    default OperatorKVStore[] getPartitionedKVStoresOrFail ( final int regionId, final String operatorId )
    {
        final OperatorKVStore[] operatorKVStores = getPartitionedKVStores( regionId, operatorId );
        if ( operatorKVStores == null )
        {
            throw new IllegalStateException(
                    "partitioned operator kv stores not found for regionId=" + regionId + " operatorId=" + operatorId );
        }

        return operatorKVStores;
    }

    void releaseDefaultKVStore ( int regionId, String operatorId );

    void releasePartitionedKVStores ( int regionId, String operatorId );

}
