package cs.bilkent.joker.engine.pipeline;


import java.util.function.Supplier;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import cs.bilkent.joker.engine.exception.InitializationException;
import cs.bilkent.joker.engine.flow.PipelineId;
import cs.bilkent.joker.engine.kvstore.OperatorKVStore;
import cs.bilkent.joker.engine.metric.PipelineReplicaMeter;
import static cs.bilkent.joker.engine.pipeline.OperatorReplicaStatus.INITIALIZATION_FAILED;
import static cs.bilkent.joker.engine.pipeline.OperatorReplicaStatus.RUNNING;
import static cs.bilkent.joker.engine.pipeline.UpstreamContext.UpstreamConnectionStatus.CLOSED;
import static cs.bilkent.joker.engine.pipeline.UpstreamContext.UpstreamConnectionStatus.OPEN;
import static cs.bilkent.joker.engine.pipeline.UpstreamContext.newInitialUpstreamContext;
import static cs.bilkent.joker.engine.pipeline.UpstreamContext.newInitialUpstreamContextWithAllPortsConnected;
import static cs.bilkent.joker.engine.pipeline.UpstreamContext.newSourceOperatorInitialUpstreamContext;
import cs.bilkent.joker.engine.tuplequeue.OperatorTupleQueue;
import cs.bilkent.joker.engine.tuplequeue.TupleQueueDrainerPool;
import cs.bilkent.joker.engine.tuplequeue.impl.drainer.NopDrainer;
import cs.bilkent.joker.operator.InitializationContext;
import cs.bilkent.joker.operator.Operator;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.impl.InvocationContextImpl;
import cs.bilkent.joker.operator.scheduling.ScheduleNever;
import cs.bilkent.joker.operator.scheduling.ScheduleWhenAvailable;
import cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.AT_LEAST;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.scheduleWhenTuplesAvailableOnAll;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.scheduleWhenTuplesAvailableOnAny;
import cs.bilkent.joker.operator.scheduling.SchedulingStrategy;
import cs.bilkent.joker.test.AbstractJokerTest;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.class )
public class OperatorReplicaInitializationTest extends AbstractJokerTest
{
    @Mock
    private Operator operator;

    @Mock
    private OperatorDef operatorDef;

    @Mock
    private TupleQueueDrainerPool drainerPool;

    private final UpstreamContext downstreamContext = newInitialUpstreamContext( CLOSED );

    private OperatorReplica operatorReplica;

    private UpstreamContext validUpstreamContext;

    @Before
    public void before () throws InstantiationException, IllegalAccessException
    {

        final PipelineReplicaId pipelineReplicaId = new PipelineReplicaId( new PipelineId( 0, 0 ), 0 );
        operatorReplica = new OperatorReplica( pipelineReplicaId,
                                               operatorDef,
                                               mock( OperatorTupleQueue.class ),
                                               mock( OperatorKVStore.class ),
                                               drainerPool,
                                               mock( Supplier.class ),
                                               mock( PipelineReplicaMeter.class ),
                                               new InvocationContextImpl() );

        when( operatorDef.getId() ).thenReturn( "op1" );
        when( operatorDef.getOutputPortCount() ).thenReturn( downstreamContext.getPortCount() );
        when( operatorDef.createOperator() ).thenReturn( operator );
        when( drainerPool.acquire( anyObject() ) ).thenReturn( new NopDrainer() );
    }

    @Test
    public void shouldInitializeOperatorReplicaWhenOperatorInitializationSucceeds_ScheduleWhenTuplesAvailable_ANY_PORT_SINGLE ()
    {
        final int inputPortCount = 1;
        final ScheduleWhenTuplesAvailable schedulingStrategy = scheduleWhenTuplesAvailableOnAny( AT_LEAST, inputPortCount, 1, 0 );
        shouldInitializeOperatorSuccessfully( inputPortCount,
                                              schedulingStrategy,
                                              newInitialUpstreamContextWithAllPortsConnected( inputPortCount ) );
    }

    @Test
    public void shouldInitializeOperatorReplicaWhenOperatorInitializationSucceeds_ScheduleWhenTuplesAvailable_ANY_PORT_MULTI ()
    {
        final int inputPortCount = 2;
        final ScheduleWhenTuplesAvailable schedulingStrategy = scheduleWhenTuplesAvailableOnAny( AT_LEAST, inputPortCount, 1, 0, 1 );
        shouldInitializeOperatorSuccessfully( inputPortCount,
                                              schedulingStrategy,
                                              newInitialUpstreamContextWithAllPortsConnected( inputPortCount ) );
    }

    @Test
    public void shouldInitializeOperatorReplicaWhenOperatorInitializationSucceeds_ScheduleWhenTuplesAvailable_ANY_PORT_MULTI_portClosed ()
    {
        final int inputPortCount = 2;
        final ScheduleWhenTuplesAvailable schedulingStrategy = scheduleWhenTuplesAvailableOnAny( AT_LEAST, inputPortCount, 1, 1 );
        shouldInitializeOperatorSuccessfully( inputPortCount, schedulingStrategy, newInitialUpstreamContext( CLOSED, OPEN ) );
    }

    @Test
    public void shouldInitializeOperatorReplicaWhenOperatorInitializationSucceeds_ScheduleWhenTuplesAvailable_ALL_PORTS ()
    {
        final int inputPortCount = 2;
        final ScheduleWhenTuplesAvailable schedulingStrategy = scheduleWhenTuplesAvailableOnAll( AT_LEAST, inputPortCount, 1, 0, 1 );
        shouldInitializeOperatorSuccessfully( inputPortCount,
                                              schedulingStrategy,
                                              newInitialUpstreamContextWithAllPortsConnected( inputPortCount ) );
    }

    @Test
    public void shouldInitializeOperatorReplicaWhenOperatorInitializationSucceeds_ScheduleWhenAvailable ()
    {
        final int inputPortCount = 0;
        shouldInitializeOperatorSuccessfully( inputPortCount, ScheduleWhenAvailable.INSTANCE, newSourceOperatorInitialUpstreamContext() );
    }

    public void shouldInitializeOperatorSuccessfully ( final int inputPortCount,
                                                       final SchedulingStrategy schedulingStrategy,
                                                       final UpstreamContext upstreamContext )
    {
        when( operatorDef.getInputPortCount() ).thenReturn( inputPortCount );
        validUpstreamContext = newInitialUpstreamContextWithAllPortsConnected( inputPortCount );

        when( operator.init( any( InitializationContext.class ) ) ).thenReturn( schedulingStrategy );

        operatorReplica.init( upstreamContext, downstreamContext );

        assertThat( operatorReplica.getStatus(), equalTo( RUNNING ) );
        assertThat( operatorReplica.getSchedulingStrategy(), equalTo( schedulingStrategy ) );
        assertThat( operatorReplica.getUpstreamContext(), equalTo( upstreamContext ) );
        assertThat( operatorReplica.getSelfUpstreamContext(), equalTo( downstreamContext ) );

        verify( drainerPool ).acquire( schedulingStrategy );
    }

    @Test
    public void shouldFailWhenOperatorInitializationReturnsInvalidSchedulingStrategy ()
    {
        when( operator.init( any( InitializationContext.class ) ) ).thenReturn( ScheduleNever.INSTANCE );

        try
        {
            operatorReplica.init( newInitialUpstreamContextWithAllPortsConnected( 1 ), downstreamContext );
            fail();
        }
        catch ( InitializationException ignored )
        {
            assertFailedInitialization();
        }
    }

    @Test
    public void shouldFailWhenOperatorInitializationReturnsInvalidSchedulingStrategy_ScheduleWhenAvailable_nonZeroInputPorts ()
    {
        shouldFailToInitializeOperator( 1, ScheduleWhenAvailable.INSTANCE, newInitialUpstreamContextWithAllPortsConnected( 1 ) );
    }

    @Test
    public void shouldFailWhenOperatorInitializationReturnsInvalidSchedulingStrategy_ScheduleWhenAvailable_UpstreamContextShutdown ()
    {
        final int inputPortCount = 1;
        shouldFailToInitializeOperator( inputPortCount,
                                        ScheduleWhenAvailable.INSTANCE,
                                        newInitialUpstreamContextWithAllPortsConnected( inputPortCount ) );
    }

    @Test
    public void shouldFailWhenOperatorInitializationReturnsInvalidSchedulingStrategy_ScheduleWhenTuplesAvailable_UpstreamContextShutdown ()
    {
        final int inputPortCount = 1;
        shouldFailToInitializeOperator( inputPortCount,
                                        scheduleWhenTuplesAvailableOnAny( AT_LEAST, inputPortCount, 1, 0 ),
                                        newInitialUpstreamContext( CLOSED ) );
    }

    @Test
    public void shouldFailWhenOperatorInitializationReturnsInvalidSchedulingStrategy_ScheduleWhenTuplesAvailable_ALL_PORTS_Shutdown ()
    {
        final int inputPortCount = 2;
        shouldFailToInitializeOperator( inputPortCount,
                                        scheduleWhenTuplesAvailableOnAll( AT_LEAST, inputPortCount, 1, 0, 1 ),
                                        newInitialUpstreamContext( CLOSED, CLOSED ) );
    }

    @Test
    public void shouldFailWhenOperatorInitializationReturnsSchedulingStrategy_withLessPortsThanUpstreamContext ()
    {
        final int inputPortCount = 2;
        shouldFailToInitializeOperator( inputPortCount,
                                        scheduleWhenTuplesAvailableOnAny( AT_LEAST, inputPortCount - 1, 2, 0 ),
                                        newInitialUpstreamContextWithAllPortsConnected( inputPortCount ) );
    }

    @Test
    public void shouldFailWhenOperatorInitializationReturnsSchedulingStrategy_withMorePortsThanUpstreamContext ()
    {
        final int inputPortCount = 1;
        shouldFailToInitializeOperator( inputPortCount,
                                        scheduleWhenTuplesAvailableOnAny( AT_LEAST, inputPortCount + 1, 2, 0 ),
                                        newInitialUpstreamContextWithAllPortsConnected( inputPortCount ) );
    }

    private void shouldFailToInitializeOperator ( final int inputPortCount,
                                                  final SchedulingStrategy schedulingStrategy,
                                                  final UpstreamContext upstreamContext )
    {
        when( operatorDef.getInputPortCount() ).thenReturn( inputPortCount );
        when( operator.init( any( InitializationContext.class ) ) ).thenReturn( schedulingStrategy );

        try
        {
            operatorReplica.init( upstreamContext, downstreamContext );
            fail();
        }
        catch ( InitializationException ignored )
        {
            assertFailedInitialization();
        }
    }

    @Test
    public void shouldSetStatusWhenOperatorInitializationFails ()
    {
        when( operator.init( anyObject() ) ).thenThrow( new RuntimeException() );

        try
        {
            operatorReplica.init( validUpstreamContext, downstreamContext );
            fail();
        }
        catch ( InitializationException ignored )
        {
            assertFailedInitialization();
        }

    }

    private void assertFailedInitialization ()
    {
        assertThat( operatorReplica.getStatus(), equalTo( INITIALIZATION_FAILED ) );
        assertThat( operatorReplica.getSelfUpstreamContext(), equalTo( downstreamContext.withAllUpstreamConnectionsClosed() ) );
    }

}
