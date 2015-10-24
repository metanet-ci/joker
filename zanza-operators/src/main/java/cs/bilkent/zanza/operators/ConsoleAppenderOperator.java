package cs.bilkent.zanza.operators;

import java.util.function.Function;

import cs.bilkent.zanza.operator.InitializationContext;
import cs.bilkent.zanza.operator.InvocationContext;
import cs.bilkent.zanza.operator.InvocationResult;
import cs.bilkent.zanza.operator.Operator;
import cs.bilkent.zanza.operator.OperatorConfig;
import cs.bilkent.zanza.operator.OperatorSpec;
import cs.bilkent.zanza.operator.OperatorType;
import cs.bilkent.zanza.operator.PortsToTuples;
import cs.bilkent.zanza.operator.SchedulingStrategy;
import cs.bilkent.zanza.operator.Tuple;
import cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable;
import static cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.ANY_NUMBER_OF_TUPLES;
import static cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.scheduleWhenTuplesAvailableOnDefaultPort;

@OperatorSpec( type = OperatorType.STATELESS, inputPortCount = 1, outputPortCount = 1 )
public class ConsoleAppenderOperator implements Operator
{

    public static final String TO_STRING_FUNCTION_CONFIG_PARAMETER = "toString";

    public static final String TUPLE_COUNT_CONFIG_PARAMETER = "tupleCount";

    private Function<Tuple, String> toStringFunction;

    private int tupleCount = ANY_NUMBER_OF_TUPLES;

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

        if ( config.contains( TUPLE_COUNT_CONFIG_PARAMETER ) )
        {
            this.tupleCount = config.getInteger( TUPLE_COUNT_CONFIG_PARAMETER );
        }

        return scheduleWhenTuplesAvailableOnDefaultPort( tupleCount );
    }

    @Override
    public InvocationResult process ( final InvocationContext invocationContext )
    {
        final PortsToTuples tuples = invocationContext.getTuples();
        tuples.getTuplesByDefaultPort().stream().map( toStringFunction ).forEach( System.out::println );

        final ScheduleWhenTuplesAvailable next = scheduleWhenTuplesAvailableOnDefaultPort( tupleCount );
        return new InvocationResult( next, tuples );
    }

}
