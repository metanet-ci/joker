package cs.bilkent.joker.operator.impl;

import static com.google.common.base.Preconditions.checkNotNull;
import cs.bilkent.joker.operator.InvocationContext;
import cs.bilkent.joker.operator.kvstore.KVStore;
import static java.util.Arrays.copyOf;


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

    public void setInvocationParameters ( final InvocationReason reason,
                                          final TuplesImpl input,
                                          final TuplesImpl output,
                                          final KVStore kvStore )
    {
        checkNotNull( reason );
        this.reason = reason;
        this.input = input;
        this.output = output;
        this.kvStore = kvStore;
    }

    public void setUpstreamConnectionStatuses ( final boolean[] upstreamConnectionStatuses )
    {
        this.upstreamConnectionStatuses = copyOf( upstreamConnectionStatuses, upstreamConnectionStatuses.length );
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
