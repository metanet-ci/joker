package cs.bilkent.zanza.operator;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Used for specifying the output of an {@link Operator#process(PortsToTuples, InvocationContext)} invocation.
 * Contains the {@link SchedulingStrategy} that will be used for the next invocation of an operator and the
 * tuples produced by an invocation of {@link Operator#process(PortsToTuples, InvocationContext)} method.
 */
public class InvocationResult
{

    private final SchedulingStrategy schedulingStrategy;

    private final PortsToTuples portsToTuples;


    public InvocationResult ( final SchedulingStrategy schedulingStrategy, final PortsToTuples portsToTuples )
    {
        checkNotNull( schedulingStrategy, "scheduling strategy can't be null" );
        checkNotNull( portsToTuples, "ports to tuples can't be null" );
        this.schedulingStrategy = schedulingStrategy;
        this.portsToTuples = portsToTuples;
    }

    public SchedulingStrategy getSchedulingStrategy ()
    {
        return schedulingStrategy;
    }

    public PortsToTuples getPortsToTuples ()
    {
        return portsToTuples;
    }

}
