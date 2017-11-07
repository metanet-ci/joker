package cs.bilkent.joker.operators;

import java.util.function.Predicate;

import cs.bilkent.joker.operator.InitializationContext;
import cs.bilkent.joker.operator.InvocationContext;
import cs.bilkent.joker.operator.Operator;
import cs.bilkent.joker.operator.OperatorConfig;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.Tuples;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.scheduleWhenTuplesAvailableOnDefaultPort;
import cs.bilkent.joker.operator.scheduling.SchedulingStrategy;
import cs.bilkent.joker.operator.spec.OperatorSpec;
import static cs.bilkent.joker.operator.spec.OperatorType.STATELESS;


/**
 * Applies the given predicate function to each input tuple and only returns the ones that satisfy the predicate
 */
@OperatorSpec( type = STATELESS, inputPortCount = 1, outputPortCount = 1 )
public class FilterOperator implements Operator
{

    public static final String PREDICATE_CONFIG_PARAMETER = "predicate";

    private static final int DEFAULT_TUPLE_COUNT_CONFIG_VALUE = 1;


    private Predicate<Tuple> predicate;

    @Override
    public SchedulingStrategy init ( final InitializationContext context )
    {
        final OperatorConfig config = context.getConfig();

        this.predicate = config.getOrFail( PREDICATE_CONFIG_PARAMETER );
        return scheduleWhenTuplesAvailableOnDefaultPort( DEFAULT_TUPLE_COUNT_CONFIG_VALUE );
    }

    @Override
    public void invoke ( final InvocationContext context )
    {
        final Tuples input = context.getInput();
        final Tuples output = context.getOutput();

        for ( Tuple tuple : input.getTuplesByDefaultPort() )
        {
            if ( predicate.test( tuple ) )
            {
                output.add( tuple );
            }
        }
    }

}
