package cs.bilkent.joker.operator.impl;

import java.util.List;

import cs.bilkent.joker.operator.InvocationCtx;
import cs.bilkent.joker.operator.Tuple.LatencyRecord;

public interface InternalInvocationCtx extends InvocationCtx
{

    void setInvocationReason ( InvocationReason reason );

    void reset ();

    int getInputCount ();

    boolean nextInput ();

    List<TuplesImpl> getInputs ();

    void setInvocationLatencyRecord ( LatencyRecord latencyRec );

    TuplesImpl getOutput ();

    void setUpstreamConnectionStatuses ( boolean[] upstreamConnectionStatuses );

}
