package cs.bilkent.zanza.operator;

import static com.google.common.base.Preconditions.checkNotNull;
import cs.bilkent.zanza.operator.scheduling.ScheduleNever;

/**
 * Used for specifying the output of an {@link Operator#process(InvocationContext)} invocation.
 * Contains the {@link SchedulingStrategy} that will be used for the next invocation of an operator and the
 * tuples produced by an invocation of {@link Operator#process(InvocationContext)} method.
 */
public class InvocationResult
{

    private final SchedulingStrategy schedulingStrategy;

    private final PortsToTuples outputTuples;

    /**
     * Requires a {@link SchedulingStrategy} that will be used for the next invocation of the operator, and tuples produced
     * by the last invocation. If the operator wants no more invocations, it must provide a {@link ScheduleNever} instance.
     *
     * @param schedulingStrategy
     *         the {@link SchedulingStrategy} for the next invocation
     * @param outputTuples
     *         the tuples produced by the current invocation
     */
    public InvocationResult ( final SchedulingStrategy schedulingStrategy, final PortsToTuples outputTuples )
    {
        checkNotNull( schedulingStrategy, "scheduling strategy can't be null" );
        checkNotNull( outputTuples, "ports to tuples can't be null" );
        this.schedulingStrategy = schedulingStrategy;
        this.outputTuples = outputTuples;
    }

    /**
     * Returns {@link SchedulingStrategy} that will be used for the next invocation of the operator
     *
     * @return {@link SchedulingStrategy} that will be used for the next invocation of the operator
     */
    public SchedulingStrategy getSchedulingStrategy ()
    {
        return schedulingStrategy;
    }

    /**
     * Returns tuples produced by the last invocation
     *
     * @return tuples produced by the last invocation
     */
    public PortsToTuples getOutputTuples ()
    {
        return outputTuples;
    }

}
