package cs.bilkent.joker.engine.pipeline;

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
import static cs.bilkent.joker.engine.pipeline.UpstreamContext.ConnectionStatus.CLOSED;
import static cs.bilkent.joker.engine.pipeline.UpstreamContext.ConnectionStatus.OPEN;
import static cs.bilkent.joker.engine.pipeline.UpstreamContext.newInitialUpstreamContext;
import static cs.bilkent.joker.engine.pipeline.UpstreamContext.newInitialUpstreamContextWithAllPortsConnected;
import static cs.bilkent.joker.engine.pipeline.UpstreamContext.newSourceOperatorInitialUpstreamContext;
import static cs.bilkent.joker.engine.pipeline.UpstreamContext.newSourceOperatorShutdownUpstreamContext;
import cs.bilkent.joker.engine.pipeline.impl.invocation.FusedInvocationContext;
import cs.bilkent.joker.engine.tuplequeue.OperatorQueue;
import cs.bilkent.joker.engine.tuplequeue.TupleQueue;
import cs.bilkent.joker.engine.tuplequeue.TupleQueueDrainerPool;
import cs.bilkent.joker.engine.tuplequeue.impl.drainer.pool.NonBlockingTupleQueueDrainerPool;
import cs.bilkent.joker.engine.tuplequeue.impl.operator.DefaultOperatorQueue;
import cs.bilkent.joker.engine.tuplequeue.impl.operator.EmptyOperatorQueue;
import cs.bilkent.joker.engine.tuplequeue.impl.queue.SingleThreadedTupleQueue;
import cs.bilkent.joker.operator.InitializationContext;
import cs.bilkent.joker.operator.InvocationContext;
import cs.bilkent.joker.operator.InvocationContext.InvocationReason;
import static cs.bilkent.joker.operator.InvocationContext.InvocationReason.INPUT_PORT_CLOSED;
import static cs.bilkent.joker.operator.InvocationContext.InvocationReason.SUCCESS;
import cs.bilkent.joker.operator.Operator;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.OperatorDefBuilder;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.impl.DefaultInvocationContext;
import cs.bilkent.joker.operator.impl.DefaultOutputCollector;
import cs.bilkent.joker.operator.impl.InternalInvocationContext;
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

        final DefaultInvocationContext statefulInvocationContext = new DefaultInvocationContext( 1,
                                                                                                 key -> null,
                                                                                                 new DefaultOutputCollector( 1 ) );

        final InternalInvocationContext[] invocationContexts = new InternalInvocationContext[] { statefulInvocationContext };

        operatorReplica = new OperatorReplica( pipelineReplicaId,
                                               operatorQueue,
                                               drainerPool,
                                               meter,
                                               statefulInvocationContext::createInputTuples,
                                               operatorDefs,
                                               invocationContexts );

        final UpstreamContext statefulUpstreamContext = newInitialUpstreamContextWithAllPortsConnected( 1 );
        final UpstreamContext statefulDownstreamContext = newInitialUpstreamContext( CLOSED );

        final UpstreamContext[] upstreamContexts = new UpstreamContext[] { statefulUpstreamContext };

        operatorReplica.init( upstreamContexts, statefulDownstreamContext );

        final StatefulOperator1 operator = (StatefulOperator1) operatorReplica.getOperator( 0 );

        final Tuple tuple1 = new Tuple();
        tuple1.set( "f", 2 );
        final Tuple tuple2 = new Tuple();
        tuple2.set( "f", 5 );

        final TuplesImpl input = new TuplesImpl( 1 );
        input.add( tuple1 );
        input.add( tuple2 );

        final TuplesImpl output = operatorReplica.invoke( true, input, statefulUpstreamContext.withConnectionClosed( 0 ) );

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

        final FusedInvocationContext mapperInvocationContext = new FusedInvocationContext( 1,
                                                                                           key -> null, new DefaultOutputCollector( 1 ) );

        final FusedInvocationContext filterInvocationContext = new FusedInvocationContext( 1, key -> null, mapperInvocationContext );
        final DefaultInvocationContext statefulInvocationContext = new DefaultInvocationContext( 1, key -> null, filterInvocationContext );

        final InternalInvocationContext[] invocationContexts = new InternalInvocationContext[] { statefulInvocationContext,
                                                                                                 filterInvocationContext,
                                                                                                 mapperInvocationContext };

        operatorReplica = new OperatorReplica( pipelineReplicaId,
                                               operatorQueue,
                                               drainerPool,
                                               meter,
                                               statefulInvocationContext::createInputTuples,
                                               operatorDefs,
                                               invocationContexts );

        final UpstreamContext statefulUpstreamContext = newInitialUpstreamContextWithAllPortsConnected( 1 );
        final UpstreamContext filterUpstreamContext = newInitialUpstreamContextWithAllPortsConnected( 1 );
        final UpstreamContext mapperUpstreamContext = newInitialUpstreamContextWithAllPortsConnected( 1 );
        final UpstreamContext mapperDownstreamContext = newInitialUpstreamContext( CLOSED );

        final UpstreamContext[] upstreamContexts = new UpstreamContext[] { statefulUpstreamContext,
                                                                           filterUpstreamContext,
                                                                           mapperUpstreamContext };

        operatorReplica.init( upstreamContexts, mapperDownstreamContext );

        final StatefulOperator1 operator = (StatefulOperator1) operatorReplica.getOperator( 0 );

        final Tuple tuple1 = new Tuple();
        tuple1.set( "f", 2 );

        final TuplesImpl input = new TuplesImpl( 1 );
        input.add( tuple1 );

        TuplesImpl output = operatorReplica.invoke( true, input, statefulUpstreamContext );

        assertNull( output );
        assertNull( operator.getLastInvocationReason() );
        assertThat( tupleQueue.size(), equalTo( 1 ) );

        final Tuple tuple2 = new Tuple();
        tuple2.set( "f", 5 );
        input.clear();
        input.add( tuple2 );

        output = operatorReplica.invoke( true, input, statefulUpstreamContext );

        assertThat( operator.getLastInvocationReason(), equalTo( SUCCESS ) );
        assertNotNull( output );
        assertThat( output.getTupleCount( 0 ), equalTo( 1 ) );

        final Tuple expected = new Tuple();
        add1Mapper.accept( tuple1, expected );

        assertThat( output.getTupleOrNull( 0, 0 ), equalTo( expected ) );

        final Tuple tuple3 = new Tuple();
        tuple3.set( "f", 7 );

        final Tuple tuple4 = new Tuple();
        tuple4.set( "f", 9 );

        input.clear();
        input.add( tuple3 );
        input.add( tuple4 );

        output = operatorReplica.invoke( true, input, statefulUpstreamContext );
        assertNotNull( output );
        assertTrue( output.isEmpty() );
    }

    @Test
    public void when_singleInputPortOperatorUpstreamContextIsUpdated_then_operatorReplicaMovesToCompletingStatus ()
    {
        final SingleThreadedTupleQueue tupleQueue = new SingleThreadedTupleQueue( 10 );
        final OperatorQueue operatorQueue = new DefaultOperatorQueue( "st", 1, SINGLE_THREADED, new TupleQueue[] { tupleQueue }, 100 );

        final OperatorDef[] operatorDefs = new OperatorDef[] { createStatefulOperator() };
        final TupleQueueDrainerPool drainerPool = new NonBlockingTupleQueueDrainerPool( new JokerConfig(), operatorDefs[ 0 ] );
        final PipelineReplicaMeter meter = new PipelineReplicaMeter( 1, pipelineReplicaId, operatorDefs[ 0 ] );

        final DefaultInvocationContext statefulInvocationContext = new DefaultInvocationContext( 1,
                                                                                                 key -> null,
                                                                                                 new DefaultOutputCollector( 1 ) );

        final InternalInvocationContext[] invocationContexts = new InternalInvocationContext[] { statefulInvocationContext };

        operatorReplica = new OperatorReplica( pipelineReplicaId,
                                               operatorQueue,
                                               drainerPool,
                                               meter,
                                               statefulInvocationContext::createInputTuples,
                                               operatorDefs,
                                               invocationContexts );

        final UpstreamContext statefulUpstreamContext = newInitialUpstreamContextWithAllPortsConnected( 1 );
        final UpstreamContext statefulDownstreamContext = newInitialUpstreamContext( CLOSED );

        final UpstreamContext[] upstreamContexts = new UpstreamContext[] { statefulUpstreamContext };

        operatorReplica.init( upstreamContexts, statefulDownstreamContext );

        final StatefulOperator1 operator = (StatefulOperator1) operatorReplica.getOperator( 0 );

        final Tuple tuple1 = new Tuple();
        tuple1.set( "f", 2 );
        ;

        final TuplesImpl input = new TuplesImpl( 1 );
        input.add( tuple1 );

        TuplesImpl output = operatorReplica.invoke( true, input, statefulUpstreamContext.withConnectionClosed( 0 ) );

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

        final DefaultInvocationContext statefulInvocationContext = new DefaultInvocationContext( 1,
                                                                                                 key -> null,
                                                                                                 new DefaultOutputCollector( 1 ) );

        final InternalInvocationContext[] invocationContexts = new InternalInvocationContext[] { statefulInvocationContext };

        operatorReplica = new OperatorReplica( pipelineReplicaId,
                                               operatorQueue,
                                               drainerPool,
                                               meter,
                                               statefulInvocationContext::createInputTuples,
                                               operatorDefs,
                                               invocationContexts );

        final UpstreamContext statefulUpstreamContext = newInitialUpstreamContextWithAllPortsConnected( 1 );
        final UpstreamContext statefulDownstreamContext = newInitialUpstreamContext( CLOSED );

        final UpstreamContext[] upstreamContexts = new UpstreamContext[] { statefulUpstreamContext };

        operatorReplica.init( upstreamContexts, statefulDownstreamContext );

        final Tuple tuple1 = new Tuple();
        tuple1.set( "f", 2 );

        final TuplesImpl input = new TuplesImpl( 1 );
        input.add( tuple1 );

        final UpstreamContext updatedUpstreamContext = statefulUpstreamContext.withConnectionClosed( 0 );
        operatorReplica.invoke( true, input, updatedUpstreamContext );
        final TuplesImpl output = operatorReplica.invoke( true, null, updatedUpstreamContext );

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

        final DefaultInvocationContext statefulInvocationContext = new DefaultInvocationContext( 0,
                                                                                                 key -> null,
                                                                                                 new DefaultOutputCollector( 1 ) );

        final InternalInvocationContext[] invocationContexts = new InternalInvocationContext[] { statefulInvocationContext };

        operatorReplica = new OperatorReplica( pipelineReplicaId,
                                               operatorQueue,
                                               drainerPool,
                                               meter,
                                               statefulInvocationContext::createInputTuples,
                                               operatorDefs,
                                               invocationContexts );

        final UpstreamContext statefulUpstreamContext = newSourceOperatorInitialUpstreamContext();
        final UpstreamContext statefulDownstreamContext = newInitialUpstreamContext( OPEN );

        final UpstreamContext[] upstreamContexts = new UpstreamContext[] { statefulUpstreamContext };

        operatorReplica.init( upstreamContexts, statefulDownstreamContext );

        final Tuple expected = new Tuple();
        expected.set( "f", 1 );

        final TuplesImpl output = operatorReplica.invoke( true, null, statefulUpstreamContext );

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

        final DefaultInvocationContext statefulInvocationContext = new DefaultInvocationContext( 0,
                                                                                                 key -> null,
                                                                                                 new DefaultOutputCollector( 1 ) );

        final InternalInvocationContext[] invocationContexts = new InternalInvocationContext[] { statefulInvocationContext };

        operatorReplica = new OperatorReplica( pipelineReplicaId,
                                               operatorQueue,
                                               drainerPool,
                                               meter,
                                               statefulInvocationContext::createInputTuples,
                                               operatorDefs,
                                               invocationContexts );

        final UpstreamContext statefulUpstreamContext = newSourceOperatorInitialUpstreamContext();
        final UpstreamContext statefulDownstreamContext = newInitialUpstreamContext( OPEN );

        final UpstreamContext[] upstreamContexts = new UpstreamContext[] { statefulUpstreamContext };

        operatorReplica.init( upstreamContexts, statefulDownstreamContext );

        final StatefulOperator0 operator = (StatefulOperator0) operatorReplica.getOperator( 0 );

        final Tuple expected = new Tuple();
        expected.set( "f", 1 );

        operatorReplica.invoke( true, null, newSourceOperatorShutdownUpstreamContext() );

        operator.lastInvocationReason = null;

        operatorReplica.invoke( true, null, newSourceOperatorShutdownUpstreamContext() );

        assertNull( operator.getLastInvocationReason() );
    }

    @Test
    public void when_inputPortIsClosedDuringSchedulingStrategyIsStillSatisfied_then_upstreamContextIsNotUpdated ()
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

        final DefaultInvocationContext statefulInvocationContext = new DefaultInvocationContext( 2,
                                                                                                 key -> null,
                                                                                                 new DefaultOutputCollector( 1 ) );

        final InternalInvocationContext[] invocationContexts = new InternalInvocationContext[] { statefulInvocationContext };

        operatorReplica = new OperatorReplica( pipelineReplicaId,
                                               operatorQueue,
                                               drainerPool,
                                               meter,
                                               statefulInvocationContext::createInputTuples,
                                               operatorDefs,
                                               invocationContexts );

        final UpstreamContext statefulUpstreamContext = newInitialUpstreamContextWithAllPortsConnected( 2 );
        final UpstreamContext statefulDownstreamContext = newInitialUpstreamContext( CLOSED );

        final UpstreamContext[] upstreamContexts = new UpstreamContext[] { statefulUpstreamContext };

        operatorReplica.init( upstreamContexts, statefulDownstreamContext );

        final StatefulOperator2 operator = (StatefulOperator2) operatorReplica.getOperator( 0 );

        final Tuple tuple1 = new Tuple();
        tuple1.set( "f", 2 );
        final Tuple tuple2 = new Tuple();
        tuple2.set( "f", 3 );

        final TuplesImpl input = new TuplesImpl( 2 );
        input.add( 1, tuple1 );
        input.add( 1, tuple2 );

        final UpstreamContext updatedUpstreamContext = statefulUpstreamContext.withConnectionClosed( 0 );
        final TuplesImpl output = operatorReplica.invoke( true, input, updatedUpstreamContext );

        assertNotNull( output );
        assertThat( output.getTuples( 0 ), equalTo( asList( tuple1, tuple2 ) ) );
        assertThat( operatorReplica.getStatus(), equalTo( RUNNING ) );
        assertThat( operator.getLastInvocationReason(), equalTo( SUCCESS ) );
        assertThat( operatorReplica.getUpstreamContext( 0 ), equalTo( statefulUpstreamContext ) );
    }

    @Test
    public void when_inputPortIsClosedDuringSchedulingStrategyIsNotSatisfied_then_upstreamContextIsUpdated ()
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

        final DefaultInvocationContext statefulInvocationContext = new DefaultInvocationContext( 2,
                                                                                                 key -> null,
                                                                                                 new DefaultOutputCollector( 1 ) );

        final InternalInvocationContext[] invocationContexts = new InternalInvocationContext[] { statefulInvocationContext };

        operatorReplica = new OperatorReplica( pipelineReplicaId,
                                               operatorQueue,
                                               drainerPool,
                                               meter,
                                               statefulInvocationContext::createInputTuples,
                                               operatorDefs,
                                               invocationContexts );

        final UpstreamContext statefulUpstreamContext = newInitialUpstreamContextWithAllPortsConnected( 2 );
        final UpstreamContext statefulDownstreamContext = newInitialUpstreamContext( CLOSED );

        final UpstreamContext[] upstreamContexts = new UpstreamContext[] { statefulUpstreamContext };

        operatorReplica.init( upstreamContexts, statefulDownstreamContext );

        final StatefulOperator2 operator = (StatefulOperator2) operatorReplica.getOperator( 0 );

        final UpstreamContext updatedUpstreamContext = statefulUpstreamContext.withConnectionClosed( 0 );
        final TuplesImpl output = operatorReplica.invoke( true, null, updatedUpstreamContext );

        assertNull( output );
        assertNull( operator.getLastInvocationReason() );
        assertThat( operatorReplica.getStatus(), equalTo( RUNNING ) );
        assertThat( operatorReplica.getUpstreamContext( 0 ), equalTo( updatedUpstreamContext ) );
    }


    @OperatorSpec( type = OperatorType.STATEFUL, inputPortCount = 0, outputPortCount = 1 )
    @OperatorSchema( outputs = @PortSchema( portIndex = 0, scope = EXACT_FIELD_SET, fields = @SchemaField( name = "f", type = Integer.class ) ) )
    public static class StatefulOperator0 implements Operator
    {

        private InvocationReason lastInvocationReason;

        private int count;

        @Override
        public SchedulingStrategy init ( final InitializationContext ctx )
        {
            return ScheduleWhenAvailable.INSTANCE;
        }

        @Override
        public void invoke ( final InvocationContext ctx )
        {
            lastInvocationReason = ctx.getReason();
            Tuple tuple = new Tuple();
            tuple.set( "f", ++count );
            ctx.output( tuple );
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
        public SchedulingStrategy init ( final InitializationContext ctx )
        {
            return scheduleWhenTuplesAvailableOnAny( TupleAvailabilityByCount.AT_LEAST, 2, 2, 0, 1 );
        }

        @Override
        public void invoke ( final InvocationContext ctx )
        {
            lastInvocationReason = ctx.getReason();
            ctx.getInputTuples( 0 ).forEach( ctx::output );
            ctx.getInputTuples( 1 ).forEach( ctx::output );
        }

        public InvocationReason getLastInvocationReason ()
        {
            return lastInvocationReason;
        }

    }

}
