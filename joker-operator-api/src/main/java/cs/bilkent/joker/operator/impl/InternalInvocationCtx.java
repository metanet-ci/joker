package cs.bilkent.joker.operator.impl;

import java.util.List;

import cs.bilkent.joker.operator.InvocationCtx;
import cs.bilkent.joker.operator.Tuple.LatencyStage;

public interface InternalInvocationCtx extends InvocationCtx
{

    void setInvocationReason ( InvocationReason reason );

    void reset ();

    int getInputCount ();

    boolean nextInput ();

    List<TuplesImpl> getInputs ();

    void setInvocationLatencyStage ( LatencyStage latencyStage );

    TuplesImpl getOutput ();

    void setUpstreamConnectionStatuses ( boolean[] upstreamConnectionStatuses );

}
