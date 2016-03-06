package cs.bilkent.zanza.engine.kvstore;

import cs.bilkent.zanza.operator.kvstore.KVStore;

public interface KVStoreProvider
{

    KVStore getKVStore ( Object key );

}
