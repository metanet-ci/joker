package cs.bilkent.zanza.engine.kvstore;

import cs.bilkent.zanza.operator.kvstore.KVStore;


public interface KVStoreContext
{

    String getOperatorId ();

    KVStore getKVStore ( Object key );

}
