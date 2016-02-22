package cs.bilkent.zanza.engine.kvstore;

import cs.bilkent.zanza.kvstore.KVStore;


public interface KVStoreContext
{

    String getOperatorId ();

    int getKVStoreCount ();

    KVStore getKVStore ( int replicaIndex );

    default KVStore getKVStore ()
    {
        return getKVStore( 0 );
    }

}
