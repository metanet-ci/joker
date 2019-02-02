package cs.bilkent.joker.operators;

import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;

import cs.bilkent.joker.operator.InitCtx;
import cs.bilkent.joker.operator.InvocationCtx;
import cs.bilkent.joker.operator.Operator;
import cs.bilkent.joker.operator.OperatorConfig;
import cs.bilkent.joker.operator.Tuple;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.scheduleWhenTuplesAvailableOnDefaultPort;
import cs.bilkent.joker.operator.scheduling.SchedulingStrategy;
import cs.bilkent.joker.operator.spec.OperatorSpec;
import static cs.bilkent.joker.operator.spec.OperatorType.STATEFUL;

/**
 * Applies the given predicate function to each input tuple and only returns the ones that satisfy the predicate
 */
@OperatorSpec( type = STATEFUL, inputPortCount = 1, outputPortCount = 1 )
public class StatefulFilterOperator implements Operator
{

    public static final String PREDICATE_CONFIG_PARAMETER = "predicate";

    private static final int DEFAULT_TUPLE_COUNT_CONFIG_VALUE = 1;


    private Predicate<Tuple> predicate;

    @Override
    public SchedulingStrategy init ( final InitCtx ctx )
    {
        final OperatorConfig config = ctx.getConfig();

        Object predicateObject = config.getOrFail( PREDICATE_CONFIG_PARAMETER );
        if ( predicateObject instanceof Supplier )
        {
            Supplier<Predicate<Tuple>> predicateFactory = (Supplier<Predicate<Tuple>>) predicateObject;
            this.predicate = predicateFactory.get();
        }
        else if ( predicateObject instanceof Predicate )
        {
            this.predicate = (Predicate<Tuple>) predicateObject;
        }
        else
        {
            throw new IllegalArgumentException( "Invalid config value for " + PREDICATE_CONFIG_PARAMETER );
        }

        return scheduleWhenTuplesAvailableOnDefaultPort( DEFAULT_TUPLE_COUNT_CONFIG_VALUE );
    }

    @Override
    public void invoke ( final InvocationCtx ctx )
    {
        final List<Tuple> tuples = ctx.getInputTuplesByDefaultPort();
        for ( int i = 0, j = tuples.size(); i < j; i++ )
        {
            final Tuple tuple = tuples.get( i );
            if ( predicate.test( tuple ) )
            {
                ctx.output( tuple );
            }
        }
    }

}
