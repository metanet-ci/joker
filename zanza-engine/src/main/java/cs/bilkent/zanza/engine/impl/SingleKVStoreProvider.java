package cs.bilkent.zanza.engine.impl;

import java.util.function.Function;

import cs.bilkent.zanza.engine.kvstore.KVStoreContext;
import cs.bilkent.zanza.kvstore.KVStore;
import cs.bilkent.zanza.operator.PortsToTuples;


public class SingleKVStoreProvider implements Function<PortsToTuples, KVStore>
{

    private final KVStoreContext kvStoreContext;

    public SingleKVStoreProvider ( final KVStoreContext kvStoreContext )
    {
        this.kvStoreContext = kvStoreContext;
    }

    @Override
    public KVStore apply ( final PortsToTuples portsToTuples )
    {
        return kvStoreContext.getKVStore();
    }

}
