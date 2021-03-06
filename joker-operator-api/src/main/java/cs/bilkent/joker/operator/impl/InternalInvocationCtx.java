package cs.bilkent.joker.operator.impl;

import java.util.List;

import cs.bilkent.joker.operator.InvocationCtx;

public interface InternalInvocationCtx extends InvocationCtx
{

    void setInvocationReason ( InvocationReason reason );

    void reset ();

    int getInputCount ();

    boolean nextInput ();

    List<TuplesImpl> getInputs ();

    OutputCollector getOutputCollector ();

    default TuplesImpl getOutput ()
    {
        return getOutputCollector().getOutputTuples();
    }

    void setUpstreamConnectionStatuses ( boolean[] upstreamConnectionStatuses );

}
