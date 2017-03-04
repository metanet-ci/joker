package cs.bilkent.joker.operator.impl;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import cs.bilkent.joker.operator.InvocationContext;
import cs.bilkent.joker.operator.kvstore.KVStore;
import static java.util.Arrays.copyOf;


public class InvocationContextImpl implements InvocationContext
{

    private InvocationReason reason;

    private TuplesImpl input;

    private TuplesImpl output;

    private List<Object> partitionKey;

    private KVStore kvStore;

    private boolean[] upstreamConnectionStatuses;

    public InvocationContextImpl ()
    {
    }

    public void setInvocationParameters ( final InvocationReason reason, final TuplesImpl input, final TuplesImpl output )
    {
        setInvocationParameters( reason, input, output, null, null );
    }

    public void setInvocationParameters ( final InvocationReason reason,
                                          final TuplesImpl input, final TuplesImpl output, final List<Object> partitionKey,
                                          final KVStore kvStore )
    {
        checkNotNull( reason );
        this.reason = reason;
        this.input = input;
        this.output = output;
        this.partitionKey = partitionKey;
        this.kvStore = kvStore;
    }

    public void resetInvocationParameters ()
    {
        this.reason = null;
        this.input = null;
        this.output = null;
        this.partitionKey = null;
        this.kvStore = null;
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
    public TuplesImpl getOutput ()
    {
        return output;
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
    public List<Object> getPartitionKey ()
    {
        return partitionKey;
    }

    @Override
    public KVStore getKVStore ()
    {
        return kvStore;
    }
}
