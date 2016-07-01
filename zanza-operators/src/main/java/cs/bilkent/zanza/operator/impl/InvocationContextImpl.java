package cs.bilkent.zanza.operator.impl;

import cs.bilkent.zanza.operator.InvocationContext;
import cs.bilkent.zanza.operator.kvstore.KVStore;


public class InvocationContextImpl implements InvocationContext
{

    private InvocationReason reason;

    private TuplesImpl input;

    private TuplesImpl output;

    private KVStore kvStore;

    private boolean[] upstreamConnectionStatuses;

    public InvocationContextImpl ()
    {
    }

    public InvocationContextImpl ( final InvocationReason reason, final TuplesImpl input, final TuplesImpl output )
    {
        this.reason = reason;
        this.input = input;
        this.output = output;
    }

    public InvocationContextImpl ( final InvocationReason reason, final TuplesImpl input, final TuplesImpl output, final KVStore kvStore )
    {
        this.input = input;
        this.output = output;
        this.reason = reason;
        this.kvStore = kvStore;
    }

    public void setReason ( final InvocationReason reason )
    {
        this.reason = reason;
    }

    public void setInvocationParameters ( final InvocationReason reason,
                                          final TuplesImpl input,
                                          final TuplesImpl output,
                                          final KVStore kvStore )
    {
        this.reason = reason;
        this.input = input;
        this.output = output;
        this.kvStore = kvStore;
    }

    public void setUpstreamConnectionStatuses ( final boolean[] upstreamConnectionStatuses )
    {
        this.upstreamConnectionStatuses = upstreamConnectionStatuses;
    }

    @Override
    public TuplesImpl getInput ()
    {
        return input;
    }

    @Override
    public InvocationReason getReason ()
    {
        return reason;
    }

    @Override
    public boolean isInputPortOpen ( final int portIndex )
    {
        return upstreamConnectionStatuses[ portIndex ];
    }

    @Override
    public boolean isInputPortClosed ( final int portIndex )
    {
        return !upstreamConnectionStatuses[ portIndex ];
    }

    @Override
    public KVStore getKVStore ()
    {
        return kvStore;
    }

    @Override
    public TuplesImpl getOutput ()
    {
        return output;
    }

}
