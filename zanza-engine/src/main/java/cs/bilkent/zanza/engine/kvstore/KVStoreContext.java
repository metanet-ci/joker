package cs.bilkent.zanza.engine.kvstore;

import cs.bilkent.zanza.kvstore.KVStore;


public interface KVStoreContext
{

    String getOperatorId ();

    KVStore getKVStore ();

    KVStore getKVStore ( Object partitionKey );

}
