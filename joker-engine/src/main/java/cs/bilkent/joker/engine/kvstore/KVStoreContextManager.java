package cs.bilkent.joker.engine.kvstore;

public interface KVStoreContextManager
{

    KVStoreContext createDefaultKVStoreContext ( int regionId, String operatorId );

    KVStoreContext[] createPartitionedKVStoreContexts ( int regionId, int replicaCount, String operatorId );

    boolean releaseDefaultKVStoreContext ( int regionId, String operatorId );

    boolean releasePartitionedKVStoreContext ( int regionId, String operatorId );

}
