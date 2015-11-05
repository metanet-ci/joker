package cs.bilkent.zanza.utils;

import cs.bilkent.zanza.kvstore.KVStore;
import cs.bilkent.zanza.operator.InvocationContext;
import cs.bilkent.zanza.operator.PortsToTuples;

public class SimpleInvocationContext implements InvocationContext
{

    private PortsToTuples tuples;

    private InvocationReason reason;

    private KVStore kvStore;

    public SimpleInvocationContext ()
    {
    }

    public SimpleInvocationContext ( final PortsToTuples tuples, final InvocationReason reason )
    {
        this.tuples = tuples;
        this.reason = reason;
    }

    public SimpleInvocationContext ( final PortsToTuples tuples, final InvocationReason reason, final KVStore kvStore )
    {
        this.tuples = tuples;
        this.reason = reason;
        this.kvStore = kvStore;
    }

    public void setTuples ( final PortsToTuples tuples )
    {
        this.tuples = tuples;
    }

    public void setReason ( final InvocationReason reason )
    {
        this.reason = reason;
    }

    public void setKvStore ( final KVStore kvStore )
    {
        this.kvStore = kvStore;
    }

    @Override
    public PortsToTuples getInputTuples ()
    {
        return tuples;
    }

    @Override
    public InvocationReason getReason ()
    {
        return reason;
    }

    @Override
    public KVStore getKVStore ()
    {
        return kvStore;
    }
}
