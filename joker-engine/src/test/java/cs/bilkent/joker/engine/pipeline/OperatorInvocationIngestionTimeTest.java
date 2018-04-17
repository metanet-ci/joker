package cs.bilkent.joker.engine.pipeline;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import org.junit.Ignore;
import org.junit.Test;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import cs.bilkent.joker.Joker;
import cs.bilkent.joker.Joker.JokerBuilder;
import cs.bilkent.joker.flow.FlowDef;
import cs.bilkent.joker.flow.FlowDefBuilder;
import static cs.bilkent.joker.impl.com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.joker.operator.InitCtx;
import cs.bilkent.joker.operator.InvocationCtx;
import cs.bilkent.joker.operator.Operator;
import cs.bilkent.joker.operator.OperatorConfig;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.OperatorDefBuilder;
import cs.bilkent.joker.operator.Tuple;
import static cs.bilkent.joker.operator.TupleAccessor.getIngestionTime;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.scheduleWhenTuplesAvailableOnDefaultPort;
import cs.bilkent.joker.operator.scheduling.SchedulingStrategy;
import cs.bilkent.joker.operator.spec.OperatorSpec;
import cs.bilkent.joker.operator.spec.OperatorType;
import cs.bilkent.joker.operators.BeaconOperator;
import static cs.bilkent.joker.operators.BeaconOperator.TUPLE_COUNT_CONFIG_PARAMETER;
import static cs.bilkent.joker.operators.BeaconOperator.TUPLE_POPULATOR_CONFIG_PARAMETER;
import cs.bilkent.joker.test.AbstractJokerTest;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class OperatorInvocationIngestionTimeTest extends AbstractJokerTest
{

    @Ignore
    @Test
    public void testIngestionTimes () throws InterruptedException, ExecutionException, TimeoutException
    {
        final Consumer<Tuple> tupleGenerator = tuple -> sleepUninterruptibly( 10, NANOSECONDS );
        final OperatorConfig beacon1Config = new OperatorConfig().set( TUPLE_POPULATOR_CONFIG_PARAMETER, tupleGenerator )
                                                                 .set( TUPLE_COUNT_CONFIG_PARAMETER, 1 );

        final OperatorDef beacon = OperatorDefBuilder.newInstance( "beacon", BeaconOperator.class ).setConfig( beacon1Config ).build();

        final OperatorDef sink = OperatorDefBuilder.newInstance( "sink", StatefulOperator.class ).build();

        final FlowDef flow = new FlowDefBuilder().add( beacon ).add( sink ).connect( "beacon", "sink" ).build();

        final Joker joker = new JokerBuilder().build();
        joker.run( flow );

        sleepUninterruptibly( 15, SECONDS );

        joker.shutdown().get( 30, SECONDS );
    }

    @OperatorSpec( type = OperatorType.STATEFUL, inputPortCount = 1, outputPortCount = 1 )
    public static class StatefulOperator implements Operator
    {

        @Override
        public SchedulingStrategy init ( final InitCtx ctx )
        {
            return scheduleWhenTuplesAvailableOnDefaultPort( 5 );
        }

        @Override
        public void invoke ( final InvocationCtx ctx )
        {
            final List<Tuple> input = ctx.getInputTuplesByDefaultPort();

            if ( ctx.isSuccessfulInvocation() )
            {
                final long ingestionTime = getIngestionTime( input.get( 4 ) );
                for ( int i = 0; i < 4; i++ )
                {
                    final Tuple tuple = input.get( i );
                    checkArgument( ingestionTime == getIngestionTime( tuple ) );
                }
            }

            input.forEach( ctx::output );
        }

    }

}
