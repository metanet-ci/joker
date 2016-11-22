package cs.bilkent.joker.engine.kvstore.impl;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.concurrent.NotThreadSafe;

import cs.bilkent.joker.engine.partition.PartitionKey;
import cs.bilkent.joker.operator.kvstore.KVStore;
import cs.bilkent.joker.operator.kvstore.impl.InMemoryKVStore;

@NotThreadSafe
public class KVStoreContainer
{

    private final int partitionId;

    private final Map<PartitionKey, KVStore> kvStores = new HashMap<>();


    KVStoreContainer ( final int partitionId )
    {
        this.partitionId = partitionId;
    }

    public int getPartitionId ()
    {
        return partitionId;
    }

    public int getKeyCount ()
    {
        return kvStores.size();
    }

    KVStore getOrCreateKVStore ( final PartitionKey key )
    {
        return kvStores.computeIfAbsent( key, k -> new InMemoryKVStore() );
    }

    public void clear ()
    {
        kvStores.values().forEach( KVStore::clear );

        kvStores.clear();
    }
}
