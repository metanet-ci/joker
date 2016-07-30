package cs.bilkent.zanza.operators;

import java.util.function.Function;

import cs.bilkent.zanza.operator.InitializationContext;
import cs.bilkent.zanza.operator.InvocationContext;
import cs.bilkent.zanza.operator.Operator;
import cs.bilkent.zanza.operator.OperatorConfig;
import cs.bilkent.zanza.operator.Tuple;
import cs.bilkent.zanza.operator.Tuples;
import static cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.scheduleWhenTuplesAvailableOnDefaultPort;
import cs.bilkent.zanza.operator.scheduling.SchedulingStrategy;
import cs.bilkent.zanza.operator.spec.OperatorSpec;
import static cs.bilkent.zanza.operator.spec.OperatorType.STATEFUL;


/**
 * Prints all input tuples to {@link System#out} and forwards them to the default output port
 */
@OperatorSpec( type = STATEFUL, inputPortCount = 1, outputPortCount = 1 )
public class ConsoleAppenderOperator implements Operator
{

    public static final String TO_STRING_FUNCTION_CONFIG_PARAMETER = "toString";

    public static final String TUPLE_COUNT_CONFIG_PARAMETER = "tupleCount";

    public static final int DEFAULT_TUPLE_COUNT_CONFIG_VALUE = 1;


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

        final int tupleCount = config.getIntegerOrDefault( TUPLE_COUNT_CONFIG_PARAMETER, DEFAULT_TUPLE_COUNT_CONFIG_VALUE );

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
    }

}
