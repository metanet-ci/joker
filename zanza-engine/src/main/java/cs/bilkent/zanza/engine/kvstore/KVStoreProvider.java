package cs.bilkent.zanza.engine.kvstore;

import cs.bilkent.zanza.kvstore.KVStore;

public interface KVStoreProvider
{

    KVStore getKVStore ( Object key );

}
