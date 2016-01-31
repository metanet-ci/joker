package cs.bilkent.zanza.engine.impl;

import java.util.function.Function;

import cs.bilkent.zanza.kvstore.KVStore;
import cs.bilkent.zanza.operator.PortsToTuples;


public class NullKvStoreProvider implements Function<PortsToTuples, KVStore>
{

    public static final NullKvStoreProvider INSTANCE = new NullKvStoreProvider();

    private NullKvStoreProvider ()
    {

    }

    @Override
    public KVStore apply ( final PortsToTuples portsToTuples )
    {
        return null;
    }

}
