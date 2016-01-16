package cs.bilkent.zanza.operators;

import java.util.function.Consumer;

import cs.bilkent.zanza.operator.InitializationContext;
import cs.bilkent.zanza.operator.InvocationContext;
import cs.bilkent.zanza.operator.InvocationResult;
import cs.bilkent.zanza.operator.Operator;
import cs.bilkent.zanza.operator.OperatorConfig;
import cs.bilkent.zanza.operator.PortsToTuples;
import cs.bilkent.zanza.operator.Tuple;
import cs.bilkent.zanza.operator.spec.OperatorSpec;
import cs.bilkent.zanza.operator.spec.OperatorType;
import cs.bilkent.zanza.scheduling.ScheduleNever;
import cs.bilkent.zanza.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount;
import static cs.bilkent.zanza.scheduling.ScheduleWhenTuplesAvailable.scheduleWhenTuplesAvailableOnDefaultPort;
import cs.bilkent.zanza.scheduling.SchedulingStrategy;


/**
 * A side-effecting operator which applies the given function to each input tuple.
 * Forwards all input tuples to default output port directly.
 * {@link Consumer} function is assumed not to mutate input tuples.
 */
@OperatorSpec( type = OperatorType.STATELESS, inputPortCount = 1, outputPortCount = 1 )
public class ForEachOperator implements Operator
{

    public static final String CONSUMER_FUNCTION_CONFIG_PARAMETER = "consumer";

    public static final String EXACT_TUPLE_COUNT_CONFIG_PARAMETER = "tupleCount";

    private Consumer<Tuple> consumerFunc;

    private int tupleCount;

    @Override
    public SchedulingStrategy init ( final InitializationContext context )
    {
        final OperatorConfig config = context.getConfig();

        this.consumerFunc = config.getOrFail( CONSUMER_FUNCTION_CONFIG_PARAMETER );
        this.tupleCount = config.getIntegerOrDefault( EXACT_TUPLE_COUNT_CONFIG_PARAMETER, 0 );

        return getTupleCountBasedSchedulingStrategy();
    }

    private SchedulingStrategy getTupleCountBasedSchedulingStrategy ()
    {
        return tupleCount > 0
               ? scheduleWhenTuplesAvailableOnDefaultPort( TupleAvailabilityByCount.EXACT, tupleCount )
               : scheduleWhenTuplesAvailableOnDefaultPort( 1 );
    }


    @Override
    public InvocationResult process ( final InvocationContext invocationContext )
    {
        final SchedulingStrategy nextScheduling = invocationContext.isSuccessfulInvocation()
                                                  ? getTupleCountBasedSchedulingStrategy()
                                                  : ScheduleNever.INSTANCE;
        final PortsToTuples input = invocationContext.getInputTuples();
        input.getTuplesByDefaultPort().stream().forEach( consumerFunc );
        return new InvocationResult( nextScheduling, input );
    }

}
