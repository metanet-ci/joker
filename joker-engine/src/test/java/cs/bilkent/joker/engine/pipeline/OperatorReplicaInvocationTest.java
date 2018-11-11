package cs.bilkent.joker.engine.pipeline;

import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Predicate;

import org.junit.Test;

import cs.bilkent.joker.engine.config.JokerConfig;
import static cs.bilkent.joker.engine.config.ThreadingPref.SINGLE_THREADED;
import cs.bilkent.joker.engine.metric.PipelineReplicaMeter;
import cs.bilkent.joker.engine.pipeline.OperatorReplicaInitializationTest.StatefulOperator1;
import static cs.bilkent.joker.engine.pipeline.OperatorReplicaInitializationTest.createFilterOperator;
import static cs.bilkent.joker.engine.pipeline.OperatorReplicaInitializationTest.createMapperOperator;
import static cs.bilkent.joker.engine.pipeline.OperatorReplicaInitializationTest.createStatefulOperator;
import static cs.bilkent.joker.engine.pipeline.OperatorReplicaStatus.COMPLETED;
import static cs.bilkent.joker.engine.pipeline.OperatorReplicaStatus.COMPLETING;
import static cs.bilkent.joker.engine.pipeline.OperatorReplicaStatus.RUNNING;
import static cs.bilkent.joker.engine.pipeline.UpstreamCtx.ConnectionStatus.CLOSED;
import static cs.bilkent.joker.engine.pipeline.UpstreamCtx.ConnectionStatus.OPEN;
import static cs.bilkent.joker.engine.pipeline.UpstreamCtx.createInitialClosedUpstreamCtx;
import static cs.bilkent.joker.engine.pipeline.UpstreamCtx.createInitialSourceUpstreamCtx;
import static cs.bilkent.joker.engine.pipeline.UpstreamCtx.createShutdownSourceUpstreamCtx;
import cs.bilkent.joker.engine.pipeline.impl.invocation.DefaultOutputCollector;
import cs.bilkent.joker.engine.pipeline.impl.invocation.FusedInvocationCtx;
import cs.bilkent.joker.engine.tuplequeue.OperatorQueue;
import cs.bilkent.joker.engine.tuplequeue.TupleQueue;
import cs.bilkent.joker.engine.tuplequeue.TupleQueueDrainerPool;
import cs.bilkent.joker.engine.tuplequeue.impl.drainer.pool.NonBlockingTupleQueueDrainerPool;
import cs.bilkent.joker.engine.tuplequeue.impl.operator.DefaultOperatorQueue;
import cs.bilkent.joker.engine.tuplequeue.impl.operator.EmptyOperatorQueue;
import cs.bilkent.joker.engine.tuplequeue.impl.queue.SingleThreadedTupleQueue;
import cs.bilkent.joker.operator.InitCtx;
import cs.bilkent.joker.operator.InvocationCtx;
import cs.bilkent.joker.operator.InvocationCtx.InvocationReason;
import static cs.bilkent.joker.operator.InvocationCtx.InvocationReason.INPUT_PORT_CLOSED;
import static cs.bilkent.joker.operator.InvocationCtx.InvocationReason.SUCCESS;
import cs.bilkent.joker.operator.Operator;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.OperatorDefBuilder;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.impl.DefaultInvocationCtx;
import cs.bilkent.joker.operator.impl.InternalInvocationCtx;
import cs.bilkent.joker.operator.impl.TuplesImpl;
import cs.bilkent.joker.operator.scheduling.ScheduleWhenAvailable;
import cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.scheduleWhenTuplesAvailableOnAny;
import cs.bilkent.joker.operator.scheduling.SchedulingStrategy;
import cs.bilkent.joker.operator.schema.annotation.OperatorSchema;
import cs.bilkent.joker.operator.schema.annotation.PortSchema;
import static cs.bilkent.joker.operator.schema.annotation.PortSchemaScope.EXACT_FIELD_SET;
import cs.bilkent.joker.operator.schema.annotation.SchemaField;
import cs.bilkent.joker.operator.spec.OperatorSpec;
import cs.bilkent.joker.operator.spec.OperatorType;
import cs.bilkent.joker.test.AbstractJokerTest;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class OperatorReplicaInvocationTest extends AbstractJokerTest
{

    private final PipelineReplicaId pipelineReplicaId = new PipelineReplicaId( 0, 0, 0 );

    private OperatorReplica operatorReplica;

    @Test
    public void when_operatorSchedulingStrategyIsSatisfied_then_operatorIsInvoked ()
    {
        final SingleThreadedTupleQueue tupleQueue = new SingleThreadedTupleQueue( 10 );
        final OperatorQueue operatorQueue = new DefaultOperatorQueue( "st", 1, SINGLE_THREADED, new TupleQueue[] { tupleQueue }, 100 );

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
        final UpstreamCtx statefulDownstreamCtx = UpstreamCtx.createInitialUpstreamCtx( CLOSED );

        final UpstreamCtx[] upstreamCtxes = new UpstreamCtx[] { statefulUpstreamCtx };

        operatorReplica.init( upstreamCtxes, statefulDownstreamCtx );

        final StatefulOperator1 operator = (StatefulOperator1) operatorReplica.getOperator( 0 );

        final Tuple tuple1 = Tuple.of( "f", 2 );
        final Tuple tuple2 = Tuple.of( "f", 5 );

        final TuplesImpl input = new TuplesImpl( 1 );
        input.add( tuple1, tuple2 );

        final TuplesImpl output = operatorReplica.invoke( input, statefulUpstreamCtx.withConnectionClosed( 0 ) );

        assertThat( operator.getLastInvocationReason(), equalTo( SUCCESS ) );
        assertNotNull( output );
        assertThat( output.getTuples( 0 ), equalTo( asList( tuple1, tuple2 ) ) );
        assertThat( operatorReplica.getStatus(), equalTo( RUNNING ) );
    }

    @Test
    public void when_firstOperatorSchedulingStrategyIsSatisfied_then_allOperatorsAreInvoked ()
    {
        final SingleThreadedTupleQueue tupleQueue = new SingleThreadedTupleQueue( 10 );
        final OperatorQueue operatorQueue = new DefaultOperatorQueue( "st", 1, SINGLE_THREADED, new TupleQueue[] { tupleQueue }, 100 );

        final Predicate<Tuple> filterEvenNumbersPredicate = tuple -> tuple.getInteger( "f" ) % 2 == 0;
        final BiConsumer<Tuple, Tuple> add1Mapper = ( input, output ) -> output.set( "f", 1 + input.getInteger( "f" ) );

        final OperatorDef[] operatorDefs = new OperatorDef[] { createStatefulOperator(),
                                                               createFilterOperator( filterEvenNumbersPredicate ),
                                                               createMapperOperator( add1Mapper ) };
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
        final UpstreamCtx mapperDownstreamCtx = UpstreamCtx.createInitialUpstreamCtx( CLOSED );

        final UpstreamCtx[] upstreamCtxes = new UpstreamCtx[] { statefulUpstreamCtx, filterUpstreamCtx, mapperUpstreamCtx };

        operatorReplica.init( upstreamCtxes, mapperDownstreamCtx );

        final StatefulOperator1 operator = (StatefulOperator1) operatorReplica.getOperator( 0 );

        final Tuple tuple1 = Tuple.of( "f", 2 );

        final TuplesImpl input = new TuplesImpl( 1 );
        input.add( tuple1 );

        TuplesImpl output = operatorReplica.invoke( input, statefulUpstreamCtx );

        assertNull( output );
        assertNull( operator.getLastInvocationReason() );
        assertThat( tupleQueue.size(), equalTo( 1 ) );

        final Tuple tuple2 = Tuple.of( "f", 5 );
        input.clear();
        input.add( tuple2 );

        output = operatorReplica.invoke( input, statefulUpstreamCtx );

        assertThat( operator.getLastInvocationReason(), equalTo( SUCCESS ) );
        assertNotNull( output );
        assertThat( output.getTupleCount( 0 ), equalTo( 1 ) );

        final Tuple expected = new Tuple();
        add1Mapper.accept( tuple1, expected );

        assertThat( output.getTupleOrNull( 0, 0 ), equalTo( expected ) );

        final Tuple tuple3 = Tuple.of( "f", 7 );
        final Tuple tuple4 = Tuple.of( "f", 9 );

        input.clear();
        input.add( tuple3, tuple4 );

        output = operatorReplica.invoke( input, statefulUpstreamCtx );
        assertNotNull( output );
        assertTrue( output.isEmpty() );
    }

    @Test
    public void when_singleInputPortOperatorUpstreamCtxIsUpdated_then_operatorReplicaMovesToCompletingStatus ()
    {
        final SingleThreadedTupleQueue tupleQueue = new SingleThreadedTupleQueue( 10 );
        final OperatorQueue operatorQueue = new DefaultOperatorQueue( "st", 1, SINGLE_THREADED, new TupleQueue[] { tupleQueue }, 100 );

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
        final UpstreamCtx statefulDownstreamCtx = UpstreamCtx.createInitialUpstreamCtx( CLOSED );

        final UpstreamCtx[] upstreamCtxes = new UpstreamCtx[] { statefulUpstreamCtx };

        operatorReplica.init( upstreamCtxes, statefulDownstreamCtx );

        final StatefulOperator1 operator = (StatefulOperator1) operatorReplica.getOperator( 0 );

        final Tuple tuple1 = Tuple.of( "f", 2 );
        ;

        final TuplesImpl input = new TuplesImpl( 1 );
        input.add( tuple1 );

        TuplesImpl output = operatorReplica.invoke( input, statefulUpstreamCtx.withConnectionClosed( 0 ) );

        assertThat( operator.getLastInvocationReason(), equalTo( INPUT_PORT_CLOSED ) );
        assertNotNull( output );
        assertThat( output.getTuples( 0 ), equalTo( singletonList( tuple1 ) ) );
        assertThat( operatorReplica.getStatus(), equalTo( COMPLETING ) );
    }

    @Test
    public void when_operatorQueueIsDrainedDuringCompletingStatus_then_operatorReplicaMovesToCompletedStatus ()
    {
        final SingleThreadedTupleQueue tupleQueue = new SingleThreadedTupleQueue( 10 );
        final OperatorQueue operatorQueue = new DefaultOperatorQueue( "st", 1, SINGLE_THREADED, new TupleQueue[] { tupleQueue }, 100 );

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
        final UpstreamCtx statefulDownstreamCtx = UpstreamCtx.createInitialUpstreamCtx( CLOSED );

        final UpstreamCtx[] upstreamCtxes = new UpstreamCtx[] { statefulUpstreamCtx };

        operatorReplica.init( upstreamCtxes, statefulDownstreamCtx );

        final Tuple tuple1 = Tuple.of( "f", 2 );

        final TuplesImpl input = new TuplesImpl( 1 );
        input.add( tuple1 );

        final UpstreamCtx updatedUpstreamCtx = statefulUpstreamCtx.withConnectionClosed( 0 );
        operatorReplica.invoke( input, updatedUpstreamCtx );
        final TuplesImpl output = operatorReplica.invoke( null, updatedUpstreamCtx );

        assertNull( output );
        assertThat( operatorReplica.getStatus(), equalTo( COMPLETED ) );
        assertThat( operatorReplica.getCompletionReason(), equalTo( INPUT_PORT_CLOSED ) );
    }

    @Test
    public void when_operatorReplicaIsInvoked_then_sourceOperatorGeneratesTuples ()
    {
        final OperatorDef[] operatorDefs = new OperatorDef[] { OperatorDefBuilder.newInstance( "st", StatefulOperator0.class ).build() };
        final OperatorQueue operatorQueue = new EmptyOperatorQueue( operatorDefs[ 0 ].getId(), 0 );
        final TupleQueueDrainerPool drainerPool = new NonBlockingTupleQueueDrainerPool( new JokerConfig(), operatorDefs[ 0 ] );
        final PipelineReplicaMeter meter = new PipelineReplicaMeter( 1, pipelineReplicaId, operatorDefs[ 0 ] );

        final DefaultInvocationCtx statefulInvocationCtx = new DefaultInvocationCtx( 0, key -> null, new DefaultOutputCollector( 1 ) );

        final InternalInvocationCtx[] invocationCtxes = new InternalInvocationCtx[] { statefulInvocationCtx };

        operatorReplica = new OperatorReplica( pipelineReplicaId,
                                               operatorQueue,
                                               drainerPool,
                                               meter,
                                               statefulInvocationCtx::createInputTuples,
                                               operatorDefs,
                                               invocationCtxes );

        final UpstreamCtx statefulUpstreamCtx = createInitialSourceUpstreamCtx();
        final UpstreamCtx statefulDownstreamCtx = UpstreamCtx.createInitialUpstreamCtx( OPEN );

        final UpstreamCtx[] upstreamCtxes = new UpstreamCtx[] { statefulUpstreamCtx };

        operatorReplica.init( upstreamCtxes, statefulDownstreamCtx );

        final Tuple expected = Tuple.of( "f", 1 );

        final TuplesImpl output = operatorReplica.invoke( null, statefulUpstreamCtx );

        assertNotNull( output );
        assertThat( output.getTuples( 0 ), equalTo( singletonList( expected ) ) );
        assertThat( operatorReplica.getStatus(), equalTo( RUNNING ) );
    }

    @Test
    public void when_operatorIsCompleted_then_itIsNotInvokedAnymore ()
    {
        final OperatorDef[] operatorDefs = new OperatorDef[] { OperatorDefBuilder.newInstance( "st", StatefulOperator0.class ).build() };
        final OperatorQueue operatorQueue = new EmptyOperatorQueue( operatorDefs[ 0 ].getId(), 0 );
        final TupleQueueDrainerPool drainerPool = new NonBlockingTupleQueueDrainerPool( new JokerConfig(), operatorDefs[ 0 ] );
        final PipelineReplicaMeter meter = new PipelineReplicaMeter( 1, pipelineReplicaId, operatorDefs[ 0 ] );

        final DefaultInvocationCtx statefulInvocationCtx = new DefaultInvocationCtx( 0, key -> null, new DefaultOutputCollector( 1 ) );

        final InternalInvocationCtx[] invocationCtxes = new InternalInvocationCtx[] { statefulInvocationCtx };

        operatorReplica = new OperatorReplica( pipelineReplicaId,
                                               operatorQueue,
                                               drainerPool,
                                               meter,
                                               statefulInvocationCtx::createInputTuples,
                                               operatorDefs,
                                               invocationCtxes );

        final UpstreamCtx statefulUpstreamCtx = createInitialSourceUpstreamCtx();
        final UpstreamCtx statefulDownstreamCtx = UpstreamCtx.createInitialUpstreamCtx( OPEN );

        final UpstreamCtx[] upstreamCtxes = new UpstreamCtx[] { statefulUpstreamCtx };

        operatorReplica.init( upstreamCtxes, statefulDownstreamCtx );

        final StatefulOperator0 operator = (StatefulOperator0) operatorReplica.getOperator( 0 );

        final Tuple expected = Tuple.of( "f", 1 );

        operatorReplica.invoke( null, createShutdownSourceUpstreamCtx() );

        operator.lastInvocationReason = null;

        operatorReplica.invoke( null, createShutdownSourceUpstreamCtx() );

        assertNull( operator.getLastInvocationReason() );
    }

    @Test
    public void when_inputPortIsClosedDuringSchedulingStrategyIsStillSatisfied_then_upstreamCtxIsNotUpdated ()
    {
        final OperatorQueue operatorQueue = new DefaultOperatorQueue( "st",
                                                                      2,
                                                                      SINGLE_THREADED,
                                                                      new TupleQueue[] { new SingleThreadedTupleQueue( 10 ),
                                                                                         new SingleThreadedTupleQueue( 10 ) },
                                                                      100 );

        final OperatorDef[] operatorDefs = new OperatorDef[] { OperatorDefBuilder.newInstance( "st", StatefulOperator2.class ).build() };
        final TupleQueueDrainerPool drainerPool = new NonBlockingTupleQueueDrainerPool( new JokerConfig(), operatorDefs[ 0 ] );
        final PipelineReplicaMeter meter = new PipelineReplicaMeter( 1, pipelineReplicaId, operatorDefs[ 0 ] );

        final DefaultInvocationCtx statefulInvocationCtx = new DefaultInvocationCtx( 2, key -> null, new DefaultOutputCollector( 1 ) );

        final InternalInvocationCtx[] invocationCtxes = new InternalInvocationCtx[] { statefulInvocationCtx };

        operatorReplica = new OperatorReplica( pipelineReplicaId,
                                               operatorQueue,
                                               drainerPool,
                                               meter,
                                               statefulInvocationCtx::createInputTuples,
                                               operatorDefs,
                                               invocationCtxes );

        final UpstreamCtx statefulUpstreamCtx = createInitialClosedUpstreamCtx( 2 );
        final UpstreamCtx statefulDownstreamCtx = UpstreamCtx.createInitialUpstreamCtx( CLOSED );

        final UpstreamCtx[] upstreamCtxes = new UpstreamCtx[] { statefulUpstreamCtx };

        operatorReplica.init( upstreamCtxes, statefulDownstreamCtx );

        final StatefulOperator2 operator = (StatefulOperator2) operatorReplica.getOperator( 0 );

        final Tuple tuple1 = Tuple.of( "f", 2 );
        final Tuple tuple2 = Tuple.of( "f", 3 );

        final TuplesImpl input = new TuplesImpl( 2 );
        input.add( 1, tuple1, tuple2 );

        final UpstreamCtx updatedUpstreamCtx = statefulUpstreamCtx.withConnectionClosed( 0 );
        final TuplesImpl output = operatorReplica.invoke( input, updatedUpstreamCtx );

        assertNotNull( output );
        assertThat( output.getTuples( 0 ), equalTo( asList( tuple1, tuple2 ) ) );
        assertThat( operatorReplica.getStatus(), equalTo( RUNNING ) );
        assertThat( operator.getLastInvocationReason(), equalTo( SUCCESS ) );
        assertThat( operatorReplica.getUpstreamCtx( 0 ), equalTo( statefulUpstreamCtx ) );
    }

    @Test
    public void when_inputPortIsClosedDuringSchedulingStrategyIsNotSatisfied_then_upstreamCtxIsUpdated ()
    {
        final OperatorQueue operatorQueue = new DefaultOperatorQueue( "st",
                                                                      2,
                                                                      SINGLE_THREADED,
                                                                      new TupleQueue[] { new SingleThreadedTupleQueue( 10 ),
                                                                                         new SingleThreadedTupleQueue( 10 ) },
                                                                      100 );

        final OperatorDef[] operatorDefs = new OperatorDef[] { OperatorDefBuilder.newInstance( "st", StatefulOperator2.class ).build() };
        final TupleQueueDrainerPool drainerPool = new NonBlockingTupleQueueDrainerPool( new JokerConfig(), operatorDefs[ 0 ] );
        final PipelineReplicaMeter meter = new PipelineReplicaMeter( 1, pipelineReplicaId, operatorDefs[ 0 ] );

        final DefaultInvocationCtx statefulInvocationCtx = new DefaultInvocationCtx( 2, key -> null, new DefaultOutputCollector( 1 ) );

        final InternalInvocationCtx[] invocationCtxes = new InternalInvocationCtx[] { statefulInvocationCtx };

        operatorReplica = new OperatorReplica( pipelineReplicaId,
                                               operatorQueue,
                                               drainerPool,
                                               meter,
                                               statefulInvocationCtx::createInputTuples,
                                               operatorDefs,
                                               invocationCtxes );

        final UpstreamCtx statefulUpstreamCtx = createInitialClosedUpstreamCtx( 2 );
        final UpstreamCtx statefulDownstreamCtx = UpstreamCtx.createInitialUpstreamCtx( CLOSED );

        final UpstreamCtx[] upstreamCtxes = new UpstreamCtx[] { statefulUpstreamCtx };

        operatorReplica.init( upstreamCtxes, statefulDownstreamCtx );

        final StatefulOperator2 operator = (StatefulOperator2) operatorReplica.getOperator( 0 );

        final UpstreamCtx updatedUpstreamCtx = statefulUpstreamCtx.withConnectionClosed( 0 );
        final TuplesImpl output = operatorReplica.invoke( null, updatedUpstreamCtx );

        assertNull( output );
        assertNull( operator.getLastInvocationReason() );
        assertThat( operatorReplica.getStatus(), equalTo( RUNNING ) );
        assertThat( operatorReplica.getUpstreamCtx( 0 ), equalTo( updatedUpstreamCtx ) );
    }


    @OperatorSpec( type = OperatorType.STATEFUL, inputPortCount = 0, outputPortCount = 1 )
    @OperatorSchema( outputs = @PortSchema( portIndex = 0, scope = EXACT_FIELD_SET, fields = @SchemaField( name = "f", type = Integer.class ) ) )
    public static class StatefulOperator0 implements Operator
    {

        private InvocationReason lastInvocationReason;

        private int count;

        @Override
        public SchedulingStrategy init ( final InitCtx ctx )
        {
            return ScheduleWhenAvailable.INSTANCE;
        }

        @Override
        public void invoke ( final InvocationCtx ctx )
        {
            lastInvocationReason = ctx.getReason();
            ctx.output( Tuple.of( "f", ++count ) );
        }

        public InvocationReason getLastInvocationReason ()
        {
            return lastInvocationReason;
        }

    }


    @OperatorSpec( type = OperatorType.STATEFUL, inputPortCount = 2, outputPortCount = 1 )
    @OperatorSchema( inputs = { @PortSchema( portIndex = 0, scope = EXACT_FIELD_SET, fields = @SchemaField( name = "f", type = Integer.class ) ),
                                @PortSchema( portIndex = 1, scope = EXACT_FIELD_SET, fields = @SchemaField( name = "f", type = Integer.class ) ) }, outputs = @PortSchema( portIndex = 0, scope = EXACT_FIELD_SET, fields = @SchemaField( name = "f", type = Integer.class ) ) )
    public static class StatefulOperator2 implements Operator
    {

        private InvocationReason lastInvocationReason;

        @Override
        public SchedulingStrategy init ( final InitCtx ctx )
        {
            return scheduleWhenTuplesAvailableOnAny( TupleAvailabilityByCount.AT_LEAST, 2, 2, 0, 1 );
        }

        @Override
        public void invoke ( final InvocationCtx ctx )
        {
            lastInvocationReason = ctx.getReason();
            final List<Tuple> tuples0 = ctx.getInputTuples( 0 );
            final List<Tuple> tuples1 = ctx.getInputTuples( 1 );
            final int c = Math.min( tuples0.size(), tuples1.size() );

            for ( int i = 0; i < c; i++ )
            {
                final Tuple t0 = tuples0.get( i ).shallowCopy();
                t0.attachTo( tuples1.get( i ) );
                ctx.output( t0 );
                final Tuple t1 = tuples1.get( i ).shallowCopy();
                t1.attachTo( tuples0.get( i ) );
                ctx.output( t1 );
            }

            for ( int i = c; i < tuples0.size(); i++ )
            {
                ctx.output( tuples0.get( i ) );
            }

            for ( int i = c; i < tuples1.size(); i++ )
            {
                ctx.output( tuples1.get( i ) );
            }
        }

        public InvocationReason getLastInvocationReason ()
        {
            return lastInvocationReason;
        }

    }

}
