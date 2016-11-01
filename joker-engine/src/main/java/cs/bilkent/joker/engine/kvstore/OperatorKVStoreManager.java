package cs.bilkent.joker.engine.kvstore;

public interface OperatorKVStoreManager
{

    OperatorKVStore createDefaultOperatorKVStore ( int regionId, String operatorId );

    OperatorKVStore[] createPartitionedOperatorKVStore ( int regionId, int replicaCount, String operatorId );

    boolean releaseDefaultOperatorKVStore ( int regionId, String operatorId );

    boolean releasePartitionedOperatorKVStore ( int regionId, String operatorId );

}
