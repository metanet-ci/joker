package cs.bilkent.zanza.engine.pipeline;

import java.util.concurrent.Future;

import cs.bilkent.zanza.operator.impl.TuplesImpl;

/**
 * Sends a pipeline instance's output tuples to its downstream.
 */
public interface DownstreamTupleSender
{

    Future<Void> send ( TuplesImpl tuples );

}
