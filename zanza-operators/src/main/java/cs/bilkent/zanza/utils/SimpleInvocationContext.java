package cs.bilkent.zanza.utils;

import cs.bilkent.zanza.kvstore.KVStore;
import cs.bilkent.zanza.operator.InvocationContext;
import cs.bilkent.zanza.operator.PortsToTuples;


public class SimpleInvocationContext implements InvocationContext
{

    private InvocationReason reason;

    private PortsToTuples tuples;

    private KVStore kvStore;

    public SimpleInvocationContext ()
    {
    }

    public SimpleInvocationContext ( final InvocationReason reason, final PortsToTuples tuples )
    {
        this.tuples = tuples;
        this.reason = reason;
    }

    public SimpleInvocationContext ( final InvocationReason reason, final PortsToTuples tuples, final KVStore kvStore )
    {
        this.tuples = tuples;
        this.reason = reason;
        this.kvStore = kvStore;
    }

    public void setReason ( final InvocationReason reason )
    {
        this.reason = reason;
    }

    public void setTuples ( final PortsToTuples tuples )
    {
        this.tuples = tuples;
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
