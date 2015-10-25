package cs.bilkent.zanza.operators;

import java.util.Random;
import java.util.function.Function;
import java.util.stream.IntStream;

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
import cs.bilkent.zanza.operator.scheduling.ScheduleNever;
import cs.bilkent.zanza.operator.scheduling.ScheduleWhenAvailable;

@OperatorSpec( type = OperatorType.STATELESS, inputPortCount = 0, outputPortCount = 1 )
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
    public InvocationResult process ( final InvocationContext invocationContext )
    {
        final PortsToTuples output;
        final SchedulingStrategy next;
        if ( invocationContext.isSuccessfulInvocation() )
        {
            output = IntStream.range( 0, tupleCount )
                              .mapToObj( ( c ) -> tupleGeneratorFunc.apply( random ) )
                              .collect( PortsToTuples.COLLECT_TO_DEFAULT_PORT );
            next = ScheduleWhenAvailable.INSTANCE;
        }
        else
        {
            output = new PortsToTuples();
            next = ScheduleNever.INSTANCE;
        }

        return new InvocationResult( next, output );
    }
}
