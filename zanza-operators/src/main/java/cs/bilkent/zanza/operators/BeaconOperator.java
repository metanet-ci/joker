package cs.bilkent.zanza.operators;

import java.util.Random;
import java.util.function.Function;
import java.util.stream.IntStream;

import cs.bilkent.zanza.operator.InvocationReason;
import cs.bilkent.zanza.operator.Operator;
import cs.bilkent.zanza.operator.OperatorConfig;
import cs.bilkent.zanza.operator.OperatorContext;
import cs.bilkent.zanza.operator.OperatorSpec;
import cs.bilkent.zanza.operator.OperatorType;
import cs.bilkent.zanza.operator.PortsToTuples;
import cs.bilkent.zanza.operator.ProcessingResult;
import cs.bilkent.zanza.operator.Tuple;
import cs.bilkent.zanza.operator.invocationreason.SuccessfulInvocation;
import cs.bilkent.zanza.operator.scheduling.ScheduleNever;
import cs.bilkent.zanza.operator.scheduling.ScheduleWhenAvailable;
import cs.bilkent.zanza.operator.scheduling.SchedulingStrategy;

@OperatorSpec( type = OperatorType.STATELESS, inputPortCount = 0, outputPortCount = 1 )
public class BeaconOperator implements Operator
{

    public static final String TUPLE_GENERATOR_CONFIG_PARAMETER = "tupleGenerator";

    public static final String TUPLE_COUNT_CONFIG_PARAMETER = "tupleCount";

    private Function<Random, Tuple> tupleGeneratorFunc;

    private Random random = new Random();

    private int tupleCount;

    @Override
    public SchedulingStrategy init ( final OperatorContext context )
    {
        final OperatorConfig config = context.getConfig();

        Object tupleGeneratorObject = config.getObject( TUPLE_GENERATOR_CONFIG_PARAMETER );
        if ( tupleGeneratorObject instanceof Function )
        {
            this.tupleGeneratorFunc = (Function) tupleGeneratorObject;
        }
        else
        {
            throw new IllegalArgumentException( "tuple generator is not provided" );
        }

        if ( config.contains( TUPLE_COUNT_CONFIG_PARAMETER ) )
        {
            this.tupleCount = config.getInteger( TUPLE_COUNT_CONFIG_PARAMETER );
        }

        return ScheduleWhenAvailable.INSTANCE;
    }

    @Override
    public ProcessingResult process ( final PortsToTuples portsToTuples, final InvocationReason reason )
    {
        final PortsToTuples output;
        final SchedulingStrategy next;
        if ( reason == SuccessfulInvocation.INSTANCE )
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

        return new ProcessingResult( next, output );
    }
}
