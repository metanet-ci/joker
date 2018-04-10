package cs.bilkent.joker.engine.pipeline;

import java.util.function.BiConsumer;
import java.util.function.Predicate;

import org.junit.Test;

import cs.bilkent.joker.engine.config.JokerConfig;
import static cs.bilkent.joker.engine.config.ThreadingPref.SINGLE_THREADED;
import cs.bilkent.joker.engine.exception.InitializationException;
import cs.bilkent.joker.engine.metric.PipelineReplicaMeter;
import cs.bilkent.joker.engine.pipeline.UpstreamCtx.ConnectionStatus;
import static cs.bilkent.joker.engine.pipeline.UpstreamCtx.createInitialClosedUpstreamCtx;
import static cs.bilkent.joker.engine.pipeline.UpstreamCtx.createSourceOperatorInitialUpstreamCtx;
import cs.bilkent.joker.engine.pipeline.impl.invocation.FusedInvocationCtx;
import cs.bilkent.joker.engine.tuplequeue.OperatorQueue;
import cs.bilkent.joker.engine.tuplequeue.TupleQueue;
import cs.bilkent.joker.engine.tuplequeue.TupleQueueDrainerPool;
import cs.bilkent.joker.engine.tuplequeue.impl.drainer.NonBlockingSinglePortDrainer;
import cs.bilkent.joker.engine.tuplequeue.impl.drainer.pool.NonBlockingTupleQueueDrainerPool;
import cs.bilkent.joker.engine.tuplequeue.impl.operator.DefaultOperatorQueue;
import cs.bilkent.joker.engine.tuplequeue.impl.queue.SingleThreadedTupleQueue;
import cs.bilkent.joker.operator.InitCtx;
import cs.bilkent.joker.operator.InvocationCtx;
import cs.bilkent.joker.operator.InvocationCtx.InvocationReason;
import cs.bilkent.joker.operator.Operator;
import cs.bilkent.joker.operator.OperatorConfig;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.OperatorDefBuilder;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.impl.DefaultInvocationCtx;
import cs.bilkent.joker.operator.impl.DefaultOutputCollector;
import cs.bilkent.joker.operator.impl.InternalInvocationCtx;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.scheduleWhenTuplesAvailableOnDefaultPort;
import cs.bilkent.joker.operator.scheduling.SchedulingStrategy;
import cs.bilkent.joker.operator.schema.runtime.OperatorRuntimeSchema;
import cs.bilkent.joker.operator.schema.runtime.OperatorRuntimeSchemaBuilder;
import cs.bilkent.joker.operator.spec.OperatorSpec;
import cs.bilkent.joker.operator.spec.OperatorType;
import cs.bilkent.joker.operators.FilterOperator;
import cs.bilkent.joker.operators.MapperOperator;
import cs.bilkent.joker.test.AbstractJokerTest;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.fail;
import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class OperatorReplicaInitializationTest extends AbstractJokerTest
{

    private final PipelineReplicaId pipelineReplicaId = new PipelineReplicaId( 0, 0, 0 );

    private static final OperatorRuntimeSchema schema = new OperatorRuntimeSchemaBuilder( 1, 1 ).addInputField( 0, "f", Integer.class )
                                                                                                .addOutputField( 0, "f", Integer.class )
                                                                                                .build();

    private OperatorReplica operatorReplica;

    @Test
    public void shouldInitializeWithSingleOperator ()
    {
        final OperatorQueue operatorQueue = new DefaultOperatorQueue( "st",
                                                                      1,
                                                                      SINGLE_THREADED,
                                                                      new TupleQueue[] { new SingleThreadedTupleQueue( 10 ) },
                                                                      100 );
        final OperatorDef[] operatorDefs = new OperatorDef[] { createStatefulOperator() };
        final TupleQueueDrainerPool drainerPool = new NonBlockingTupleQueueDrainerPool( new JokerConfig(), operatorDefs[ 0 ] );
        final PipelineReplicaMeter meter = new PipelineReplicaMeter( 1, pipelineReplicaId, operatorDefs[ 0 ] );

        final DefaultInvocationCtx statefulInvocationCtx = new DefaultInvocationCtx( 1, key -> null, new DefaultOutputCollector( 1 ) );

        final InternalInvocationCtx[] invocationCtxes = new InternalInvocationCtx[] { statefulInvocationCtx };

        operatorReplica = new OperatorReplica( pipelineReplicaId,
                                               operatorQueue,
                                               drainerPool,
                                               meter,
                                               statefulInvocationCtx::createInputTuples,
                                               operatorDefs,
                                               invocationCtxes );

        final UpstreamCtx statefulUpstreamCtx = createInitialClosedUpstreamCtx( 1 );
        final UpstreamCtx mapperDownstreamCtx = UpstreamCtx.createInitialUpstreamCtx( ConnectionStatus.CLOSED );

        final UpstreamCtx[] upstreamCtxes = new UpstreamCtx[] { statefulUpstreamCtx };

        final SchedulingStrategy[] schedulingStrategies = operatorReplica.init( upstreamCtxes, mapperDownstreamCtx );

        assertThat( schedulingStrategies.length, equalTo( 1 ) );
        assertThat( schedulingStrategies[ 0 ], equalTo( scheduleWhenTuplesAvailableOnDefaultPort( 2 ) ) );
        assertThat( operatorReplica.getOperatorDef( 0 ), equalTo( operatorDefs[ 0 ] ) );
        assertThat( operatorReplica.getInvocationCtx( 0 ), equalTo( statefulInvocationCtx ) );
        assertThat( operatorReplica.getDownstreamCtx(), equalTo( mapperDownstreamCtx ) );
        assertThat( operatorReplica.getStatus(), equalTo( OperatorReplicaStatus.RUNNING ) );
        assertTrue( operatorReplica.getDrainer() instanceof NonBlockingSinglePortDrainer );
    }


    @Test
    public void shouldInitializeWithFusedOperators ()
    {
        final OperatorQueue operatorQueue = new DefaultOperatorQueue( "st",
                                                                      1,
                                                                      SINGLE_THREADED,
                                                                      new TupleQueue[] { new SingleThreadedTupleQueue( 10 ) },
                                                                      100 );
        final OperatorDef[] operatorDefs = new OperatorDef[] { createStatefulOperator(), createFilterOperator(), createMapperOperator() };
        final TupleQueueDrainerPool drainerPool = new NonBlockingTupleQueueDrainerPool( new JokerConfig(), operatorDefs[ 0 ] );
        final PipelineReplicaMeter meter = new PipelineReplicaMeter( 1, pipelineReplicaId, operatorDefs[ 0 ] );

        final FusedInvocationCtx mapperInvocationCtx = new FusedInvocationCtx( 1, key -> null, new DefaultOutputCollector( 1 ) );

        final FusedInvocationCtx filterInvocationCtx = new FusedInvocationCtx( 1, key -> null, mapperInvocationCtx );
        final DefaultInvocationCtx statefulInvocationCtx = new DefaultInvocationCtx( 1, key -> null, filterInvocationCtx );

        final InternalInvocationCtx[] invocationCtxes = new InternalInvocationCtx[] { statefulInvocationCtx,
                                                                                      filterInvocationCtx,
                                                                                      mapperInvocationCtx };

        operatorReplica = new OperatorReplica( pipelineReplicaId,
                                               operatorQueue,
                                               drainerPool,
                                               meter,
                                               statefulInvocationCtx::createInputTuples,
                                               operatorDefs,
                                               invocationCtxes );

        final UpstreamCtx statefulUpstreamCtx = createInitialClosedUpstreamCtx( 1 );
        final UpstreamCtx filterUpstreamCtx = createInitialClosedUpstreamCtx( 1 );
        final UpstreamCtx mapperUpstreamCtx = createInitialClosedUpstreamCtx( 1 );
        final UpstreamCtx mapperDownstreamCtx = UpstreamCtx.createInitialUpstreamCtx( ConnectionStatus.CLOSED );

        final UpstreamCtx[] upstreamCtxes = new UpstreamCtx[] { statefulUpstreamCtx, filterUpstreamCtx, mapperUpstreamCtx };

        final SchedulingStrategy[] schedulingStrategies = operatorReplica.init( upstreamCtxes, mapperDownstreamCtx );

        assertThat( schedulingStrategies.length, equalTo( 3 ) );
        assertThat( schedulingStrategies[ 0 ], equalTo( scheduleWhenTuplesAvailableOnDefaultPort( 2 ) ) );
        assertThat( schedulingStrategies[ 1 ], equalTo( scheduleWhenTuplesAvailableOnDefaultPort( 1 ) ) );
        assertThat( schedulingStrategies[ 2 ], equalTo( scheduleWhenTuplesAvailableOnDefaultPort( 1 ) ) );
        assertThat( operatorReplica.getOperatorDef( 0 ), equalTo( operatorDefs[ 0 ] ) );
        assertThat( operatorReplica.getOperatorDef( 1 ), equalTo( operatorDefs[ 1 ] ) );
        assertThat( operatorReplica.getOperatorDef( 2 ), equalTo( operatorDefs[ 2 ] ) );
        assertThat( operatorReplica.getInvocationCtx( 0 ), equalTo( statefulInvocationCtx ) );
        assertThat( operatorReplica.getInvocationCtx( 1 ), equalTo( filterInvocationCtx ) );
        assertThat( operatorReplica.getInvocationCtx( 2 ), equalTo( mapperInvocationCtx ) );
        assertThat( operatorReplica.getDownstreamCtx(), equalTo( mapperDownstreamCtx ) );
        assertThat( operatorReplica.getStatus(), equalTo( OperatorReplicaStatus.RUNNING ) );
        assertTrue( operatorReplica.getDrainer() instanceof NonBlockingSinglePortDrainer );
    }

    @Test
    public void shouldFailWhenUpstreamContextMismatchesOperatorSchedulingStrategy ()
    {
        final OperatorQueue operatorQueue = new DefaultOperatorQueue( "st",
                                                                      1,
                                                                      SINGLE_THREADED,
                                                                      new TupleQueue[] { new SingleThreadedTupleQueue( 10 ) },
                                                                      100 );
        final OperatorDef[] operatorDefs = new OperatorDef[] { createStatefulOperator(), createFilterOperator(), createMapperOperator() };
        final TupleQueueDrainerPool drainerPool = new NonBlockingTupleQueueDrainerPool( new JokerConfig(), operatorDefs[ 0 ] );
        final PipelineReplicaMeter meter = new PipelineReplicaMeter( 1, pipelineReplicaId, operatorDefs[ 0 ] );

        final FusedInvocationCtx mapperInvocationCtx = new FusedInvocationCtx( 1, key -> null, new DefaultOutputCollector( 1 ) );

        final FusedInvocationCtx filterInvocationCtx = new FusedInvocationCtx( 1, key -> null, mapperInvocationCtx );
        final DefaultInvocationCtx statefulInvocationCtx = new DefaultInvocationCtx( 1, key -> null, filterInvocationCtx );

        final InternalInvocationCtx[] invocationCtxes = new InternalInvocationCtx[] { statefulInvocationCtx,
                                                                                      filterInvocationCtx,
                                                                                      mapperInvocationCtx };

        operatorReplica = new OperatorReplica( pipelineReplicaId,
                                               operatorQueue,
                                               drainerPool,
                                               meter,
                                               statefulInvocationCtx::createInputTuples,
                                               operatorDefs,
                                               invocationCtxes );

        final UpstreamCtx statefulUpstreamCtx = createSourceOperatorInitialUpstreamCtx();
        final UpstreamCtx filterUpstreamCtx = createInitialClosedUpstreamCtx( 1 );
        final UpstreamCtx mapperUpstreamCtx = createInitialClosedUpstreamCtx( 1 );
        final UpstreamCtx mapperDownstreamCtx = UpstreamCtx.createInitialUpstreamCtx( ConnectionStatus.OPEN );

        final UpstreamCtx[] upstreamCtxes = new UpstreamCtx[] { statefulUpstreamCtx, filterUpstreamCtx, mapperUpstreamCtx };

        try
        {
            operatorReplica.init( upstreamCtxes, mapperDownstreamCtx );
            fail();
        }
        catch ( InitializationException e )
        {
            assertThat( operatorReplica.getStatus(), equalTo( OperatorReplicaStatus.INITIALIZATION_FAILED ) );
            assertFalse( operatorReplica.getDownstreamCtx().isOpenConnectionPresent() );
        }
    }

    static OperatorDef createStatefulOperator ()
    {

        return OperatorDefBuilder.newInstance( "st", StatefulOperator1.class ).setExtendingSchema( schema ).build();
    }

    static OperatorDef createFilterOperator ()
    {
        final OperatorConfig config = new OperatorConfig().set( FilterOperator.PREDICATE_CONFIG_PARAMETER,
                                                                (Predicate<Tuple>) tuple -> true );
        return OperatorDefBuilder.newInstance( "f", FilterOperator.class ).setExtendingSchema( schema ).setConfig( config ).build();
    }

    static OperatorDef createFilterOperator ( final Predicate<Tuple> predicate )
    {
        final OperatorConfig config = new OperatorConfig().set( FilterOperator.PREDICATE_CONFIG_PARAMETER, predicate );
        return OperatorDefBuilder.newInstance( "f", FilterOperator.class ).setExtendingSchema( schema ).setConfig( config ).build();
    }

    static OperatorDef createMapperOperator ()
    {
        final OperatorConfig config = new OperatorConfig().set( MapperOperator.MAPPER_CONFIG_PARAMETER,
                                                                (BiConsumer<Tuple, Tuple>) ( input, output ) -> output.set( "f",
                                                                                                                            input.get( "f" ) ) );
        return OperatorDefBuilder.newInstance( "m", MapperOperator.class ).setExtendingSchema( schema ).setConfig( config ).build();
    }

    static OperatorDef createMapperOperator ( final BiConsumer<Tuple, Tuple> mapperFunction )
    {
        final OperatorConfig config = new OperatorConfig().set( MapperOperator.MAPPER_CONFIG_PARAMETER, mapperFunction );
        return OperatorDefBuilder.newInstance( "m", MapperOperator.class ).setExtendingSchema( schema ).setConfig( config ).build();
    }


    @OperatorSpec( type = OperatorType.STATEFUL, inputPortCount = 1, outputPortCount = 1 )
    public static class StatefulOperator1 implements Operator
    {

        private InvocationReason lastInvocationReason;

        private boolean shutdown;

        @Override
        public SchedulingStrategy init ( final InitCtx ctx )
        {
            return scheduleWhenTuplesAvailableOnDefaultPort( 2 );
        }

        @Override
        public void invoke ( final InvocationCtx ctx )
        {
            lastInvocationReason = ctx.getReason();
            ctx.getInputTuplesByDefaultPort().forEach( ctx::output );
        }

        @Override
        public void shutdown ()
        {
            shutdown = true;
        }

        public InvocationReason getLastInvocationReason ()
        {
            return lastInvocationReason;
        }

        public boolean isShutdown ()
        {
            return shutdown;
        }

    }

}
