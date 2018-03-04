package cs.bilkent.joker.operators;

import java.util.function.Consumer;

import cs.bilkent.joker.operator.InitializationContext;
import cs.bilkent.joker.operator.InvocationContext;
import cs.bilkent.joker.operator.Operator;
import cs.bilkent.joker.operator.OperatorConfig;
import cs.bilkent.joker.operator.Tuple;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.AT_LEAST;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.scheduleWhenTuplesAvailableOnDefaultPort;
import cs.bilkent.joker.operator.scheduling.SchedulingStrategy;
import cs.bilkent.joker.operator.spec.OperatorSpec;
import static cs.bilkent.joker.operator.spec.OperatorType.STATEFUL;


/**
 * A side-effecting operator which applies the given function to each input tuple.
 * Forwards all input tuples to default output port directly.
 * {@link Consumer} function is assumed not to mutate input tuples.
 */
@OperatorSpec( type = STATEFUL, inputPortCount = 1, outputPortCount = 1 )
public class ForEachOperator implements Operator
{

    public static final String CONSUMER_FUNCTION_CONFIG_PARAMETER = "consumer";

    public static final String TUPLE_COUNT_CONFIG_PARAMETER = "tupleCount";

    public static final int DEFAULT_TUPLE_COUNT = 1;


    private Consumer<Tuple> consumerFunc;

    @Override
    public SchedulingStrategy init ( final InitializationContext ctx )
    {
        final OperatorConfig config = ctx.getConfig();

        this.consumerFunc = config.getOrFail( CONSUMER_FUNCTION_CONFIG_PARAMETER );
        final int tupleCount = config.getIntegerOrDefault( TUPLE_COUNT_CONFIG_PARAMETER, DEFAULT_TUPLE_COUNT );

        return tupleCount > 0
               ? scheduleWhenTuplesAvailableOnDefaultPort( AT_LEAST, tupleCount )
               : scheduleWhenTuplesAvailableOnDefaultPort( 1 );
    }


    @Override
    public void invoke ( final InvocationContext ctx )
    {
        for ( Tuple tuple : ctx.getInputTuplesByDefaultPort() )
        {
            consumerFunc.accept( tuple );
            ctx.output( tuple );
        }
    }

}
