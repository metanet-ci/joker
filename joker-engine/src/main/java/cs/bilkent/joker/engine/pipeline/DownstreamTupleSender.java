package cs.bilkent.joker.engine.pipeline;

import java.util.concurrent.Future;

import cs.bilkent.joker.operator.impl.TuplesImpl;

/**
 * Sends a pipeline instance's output tuples to its downstream.
 */
public interface DownstreamTupleSender
{

    Future<Void> send ( TuplesImpl tuples );

}
