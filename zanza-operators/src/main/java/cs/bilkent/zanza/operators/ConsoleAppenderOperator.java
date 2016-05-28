package cs.bilkent.zanza.operators;

import java.util.function.Function;

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
 * Prints all input tuples to {@link System#out} and forwards them to the default output port
 */
@OperatorSpec( type = OperatorType.STATELESS, inputPortCount = 1, outputPortCount = 1 )
public class ConsoleAppenderOperator implements Operator
{

    public static final String TO_STRING_FUNCTION_CONFIG_PARAMETER = "toString";

    public static final String TUPLE_COUNT_CONFIG_PARAMETER = "tupleCount";


    private Function<Tuple, String> toStringFunction;

    @Override
    public SchedulingStrategy init ( final InitializationContext context )
    {
        final OperatorConfig config = context.getConfig();

        Object toStringObject = config.getObject( TO_STRING_FUNCTION_CONFIG_PARAMETER );

        if ( toStringObject instanceof Function )
        {
            this.toStringFunction = (Function<Tuple, String>) toStringObject;
        }
        else
        {
            this.toStringFunction = Tuple::toString;
        }

        final int tupleCount = config.getIntegerOrDefault( TUPLE_COUNT_CONFIG_PARAMETER, 1 );

        return scheduleWhenTuplesAvailableOnDefaultPort( tupleCount );
    }

    @Override
    public void invoke ( final InvocationContext invocationContext )
    {
        final Tuples input = invocationContext.getInput();
        final Tuples output = invocationContext.getOutput();
        for ( Tuple tuple : input.getTuplesByDefaultPort() )
        {
            System.out.println( toStringFunction.apply( tuple ) );
            output.add( tuple );
        }

        if ( invocationContext.isErroneousInvocation() )
        {
            invocationContext.setNextSchedulingStrategy( ScheduleNever.INSTANCE );
        }
    }

}
