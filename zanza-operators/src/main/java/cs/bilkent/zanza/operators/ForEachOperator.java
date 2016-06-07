package cs.bilkent.zanza.operators;

import java.util.function.Consumer;

import cs.bilkent.zanza.operator.InitializationContext;
import cs.bilkent.zanza.operator.InvocationContext;
import cs.bilkent.zanza.operator.Operator;
import cs.bilkent.zanza.operator.OperatorConfig;
import cs.bilkent.zanza.operator.Tuple;
import cs.bilkent.zanza.operator.Tuples;
import cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount;
import static cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.scheduleWhenTuplesAvailableOnDefaultPort;
import cs.bilkent.zanza.operator.scheduling.SchedulingStrategy;
import cs.bilkent.zanza.operator.spec.OperatorSpec;
import static cs.bilkent.zanza.operator.spec.OperatorType.STATELESS;


/**
 * A side-effecting operator which applies the given function to each input tuple.
 * Forwards all input tuples to default output port directly.
 * {@link Consumer} function is assumed not to mutate input tuples.
 */
@OperatorSpec( type = STATELESS, inputPortCount = 1, outputPortCount = 1 )
public class ForEachOperator implements Operator
{

    public static final String CONSUMER_FUNCTION_CONFIG_PARAMETER = "consumer";

    public static final String EXACT_TUPLE_COUNT_CONFIG_PARAMETER = "tupleCount";


    private Consumer<Tuple> consumerFunc;

    @Override
    public SchedulingStrategy init ( final InitializationContext context )
    {
        final OperatorConfig config = context.getConfig();

        this.consumerFunc = config.getOrFail( CONSUMER_FUNCTION_CONFIG_PARAMETER );
        final int tupleCount = config.getIntegerOrDefault( EXACT_TUPLE_COUNT_CONFIG_PARAMETER, 1 );

        return tupleCount > 0
               ? scheduleWhenTuplesAvailableOnDefaultPort( TupleAvailabilityByCount.EXACT, tupleCount )
               : scheduleWhenTuplesAvailableOnDefaultPort( 1 );
    }


    @Override
    public void invoke ( final InvocationContext invocationContext )
    {
        final Tuples input = invocationContext.getInput();
        final Tuples output = invocationContext.getOutput();

        input.getTuplesByDefaultPort().stream().forEach( tuple -> {
            consumerFunc.accept( tuple );
            output.add( tuple );
        } );
    }

}
