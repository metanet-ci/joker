package cs.bilkent.zanza.operators;

import java.util.stream.IntStream;

import cs.bilkent.zanza.operator.InitializationContext;
import cs.bilkent.zanza.operator.InvocationContext;
import cs.bilkent.zanza.operator.InvocationResult;
import cs.bilkent.zanza.operator.Operator;
import cs.bilkent.zanza.operator.OperatorSpec;
import cs.bilkent.zanza.operator.OperatorType;
import cs.bilkent.zanza.operator.SchedulingStrategy;
import cs.bilkent.zanza.operator.scheduling.ScheduleNever;
import static cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.AT_LEAST;
import static cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.scheduleWhenTuplesAvailableOnAny;

@OperatorSpec( type = OperatorType.STATELESS, outputPortCount = 1 )
public class UnionOperator implements Operator
{

    private int[] inputPorts;

    @Override
    public SchedulingStrategy init ( final InitializationContext context )
    {
        this.inputPorts = IntStream.range( 0, context.getConfig().getInputPortCount() ).toArray();
        return getSchedulingStrategyForInputPorts();
    }

    private SchedulingStrategy getSchedulingStrategyForInputPorts ()
    {
        return scheduleWhenTuplesAvailableOnAny( AT_LEAST, 1, inputPorts );
    }

    @Override
    public InvocationResult process ( final InvocationContext invocationContext )
    {
        final SchedulingStrategy nextStrategy = invocationContext.isSuccessfulInvocation()
                                                ? getSchedulingStrategyForInputPorts()
                                                : ScheduleNever.INSTANCE;
        return new InvocationResult( nextStrategy, invocationContext.getTuples() );
    }
}
