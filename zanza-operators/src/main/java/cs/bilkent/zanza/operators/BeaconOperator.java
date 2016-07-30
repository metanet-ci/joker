package cs.bilkent.zanza.operators;

import java.util.Random;
import java.util.function.Function;
import java.util.stream.IntStream;

import cs.bilkent.zanza.operator.InitializationContext;
import cs.bilkent.zanza.operator.InvocationContext;
import cs.bilkent.zanza.operator.Operator;
import cs.bilkent.zanza.operator.OperatorConfig;
import cs.bilkent.zanza.operator.Tuple;
import cs.bilkent.zanza.operator.Tuples;
import cs.bilkent.zanza.operator.scheduling.ScheduleWhenAvailable;
import cs.bilkent.zanza.operator.scheduling.SchedulingStrategy;
import cs.bilkent.zanza.operator.spec.OperatorSpec;
import static cs.bilkent.zanza.operator.spec.OperatorType.STATEFUL;


/**
 * Produces output tuples on each invocation using the provided tuple generator function
 */
@OperatorSpec( type = STATEFUL, inputPortCount = 0, outputPortCount = 1 )
public class BeaconOperator implements Operator
{

    public static final String TUPLE_GENERATOR_CONFIG_PARAMETER = "tupleGenerator";

    public static final String TUPLE_COUNT_CONFIG_PARAMETER = "tupleCount";


    private Function<Random, Tuple> tupleGeneratorFunc;

    private Random random = new Random();

    private int tupleCount;

    @Override
    public SchedulingStrategy init ( final InitializationContext context )
    {
        final OperatorConfig config = context.getConfig();

        this.tupleGeneratorFunc = config.getOrFail( TUPLE_GENERATOR_CONFIG_PARAMETER );
        this.tupleCount = config.getOrFail( TUPLE_COUNT_CONFIG_PARAMETER );

        return ScheduleWhenAvailable.INSTANCE;
    }

    @Override
    public void invoke ( final InvocationContext invocationContext )
    {
        final Tuples output = invocationContext.getOutput();

        IntStream.range( 0, tupleCount ).mapToObj( ( c ) -> tupleGeneratorFunc.apply( random ) ).forEach( output::add );
    }
}
