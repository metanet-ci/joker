package cs.bilkent.zanza.operator.impl;

import cs.bilkent.zanza.operator.InvocationContext;
import cs.bilkent.zanza.operator.PortsToTuples;
import cs.bilkent.zanza.operator.kvstore.KVStore;


public class InvocationContextImpl implements InvocationContext
{

    private InvocationReason reason;

    private PortsToTuples tuples;

    private KVStore kvStore;

    public InvocationContextImpl ()
    {
    }

    public InvocationContextImpl ( final InvocationReason reason, final PortsToTuples tuples )
    {
        this.tuples = tuples;
        this.reason = reason;
    }

    public InvocationContextImpl ( final InvocationReason reason, final PortsToTuples tuples, final KVStore kvStore )
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
