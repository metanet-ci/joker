package cs.bilkent.joker.engine.pipeline;

import java.util.function.Consumer;

import cs.bilkent.joker.operator.impl.TuplesImpl;

/**
 * Sends a pipeline instance's output tuples to its downstream.
 */
@FunctionalInterface
public interface DownstreamCollector extends Consumer<TuplesImpl>
{
}
