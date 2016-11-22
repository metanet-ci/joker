package cs.bilkent.joker.engine.pipeline;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import cs.bilkent.joker.operator.impl.TuplesImpl;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Sends a pipeline instance's output tuples to its downstream.
 */
public interface DownstreamTupleSender
{

    long OFFER_TIMEOUT = 1;

    TimeUnit OFFER_TIME_UNIT = MILLISECONDS;


    Future<Void> send ( TuplesImpl tuples );

}
