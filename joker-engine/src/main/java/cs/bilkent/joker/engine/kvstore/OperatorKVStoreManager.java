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

    OperatorKVStore[] getPartitionedOperatorKVStores ( final int regionId, final String operatorId );

    default OperatorKVStore[] getPartitionedOperatorKVStoresOrFail ( final int regionId, final String operatorId )
    {
        final OperatorKVStore[] operatorKVStores = getPartitionedOperatorKVStores( regionId, operatorId );
        if ( operatorKVStores == null )
        {
            throw new IllegalStateException( "partitioned operator kv stores not found for regionId=" + regionId + " operatorId="
                                             + operatorId );
        }

        return operatorKVStores;
    }

    void releaseDefaultOperatorKVStore ( int regionId, String operatorId );

    void releasePartitionedOperatorKVStores ( int regionId, String operatorId );

}
