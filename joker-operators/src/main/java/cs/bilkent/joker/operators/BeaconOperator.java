package cs.bilkent.joker.operators;

import java.util.Random;
import java.util.function.Function;
import java.util.stream.IntStream;

import cs.bilkent.joker.operator.InitializationContext;
import cs.bilkent.joker.operator.InvocationContext;
import cs.bilkent.joker.operator.Operator;
import cs.bilkent.joker.operator.OperatorConfig;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.Tuples;
import cs.bilkent.joker.operator.scheduling.ScheduleWhenAvailable;
import cs.bilkent.joker.operator.scheduling.SchedulingStrategy;
import cs.bilkent.joker.operator.spec.OperatorSpec;
import static cs.bilkent.joker.operator.spec.OperatorType.STATEFUL;


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
