package cs.bilkent.zanza.operators;

import java.util.function.Predicate;

import cs.bilkent.zanza.operator.InitializationContext;
import cs.bilkent.zanza.operator.InvocationContext;
import cs.bilkent.zanza.operator.Operator;
import cs.bilkent.zanza.operator.OperatorConfig;
import cs.bilkent.zanza.operator.Tuple;
import cs.bilkent.zanza.operator.Tuples;
import cs.bilkent.zanza.operator.scheduling.ScheduleNever;
import static cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.scheduleWhenTuplesAvailableOnDefaultPort;
import cs.bilkent.zanza.operator.scheduling.SchedulingStrategy;
import cs.bilkent.zanza.operator.spec.OperatorSpec;
import cs.bilkent.zanza.operator.spec.OperatorType;


/**
 * Applies the given predicate function to each input tuple and only returns the ones that satisfy the predicate
 */
@OperatorSpec( type = OperatorType.STATELESS, inputPortCount = 1, outputPortCount = 1 )
public class FilterOperator implements Operator
{

    public static final String PREDICATE_CONFIG_PARAMETER = "predicate";

    public static final String TUPLE_COUNT_CONFIG_PARAMETER = "tupleCount";


    private Predicate<Tuple> predicate;

    @Override
    public SchedulingStrategy init ( final InitializationContext context )
    {
        final OperatorConfig config = context.getConfig();

        this.predicate = config.getOrFail( PREDICATE_CONFIG_PARAMETER );
        final int tupleCount = config.getIntegerOrDefault( TUPLE_COUNT_CONFIG_PARAMETER, 1 );
        return scheduleWhenTuplesAvailableOnDefaultPort( tupleCount );
    }

    @Override
    public void invoke ( final InvocationContext invocationContext )
    {
        final Tuples input = invocationContext.getInput();
        final Tuples output = invocationContext.getOutput();

        input.getTuplesByDefaultPort().stream().filter( predicate ).forEach( output::add );

        if ( invocationContext.isErroneousInvocation() )
        {
            invocationContext.setNextSchedulingStrategy( ScheduleNever.INSTANCE );
        }
    }

}
