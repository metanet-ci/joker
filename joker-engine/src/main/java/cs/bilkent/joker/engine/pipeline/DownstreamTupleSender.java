package cs.bilkent.joker.engine.pipeline;

import cs.bilkent.joker.operator.impl.TuplesImpl;

/**
 * Sends a pipeline instance's output tuples to its downstream.
 */
public interface DownstreamTupleSender
{

    void send ( TuplesImpl tuples );

}
