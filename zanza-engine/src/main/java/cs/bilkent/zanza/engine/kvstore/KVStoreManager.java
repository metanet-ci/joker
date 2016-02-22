package cs.bilkent.zanza.engine.kvstore;

public interface KVStoreManager
{

    KVStoreContext createKVStoreContext ( String operatorId, int replicaCount );

    boolean releaseKVStoreContext ( String operatorId );

}
