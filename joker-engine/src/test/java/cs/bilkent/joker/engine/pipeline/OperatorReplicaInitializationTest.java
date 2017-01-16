package cs.bilkent.joker.engine.pipeline;


import java.util.Arrays;
import java.util.function.Supplier;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import cs.bilkent.joker.engine.exception.InitializationException;
import cs.bilkent.joker.engine.kvstore.OperatorKVStore;
import cs.bilkent.joker.engine.metric.PipelineReplicaMeter;
import static cs.bilkent.joker.engine.pipeline.OperatorReplicaStatus.INITIALIZATION_FAILED;
import static cs.bilkent.joker.engine.pipeline.OperatorReplicaStatus.RUNNING;
import static cs.bilkent.joker.engine.pipeline.UpstreamConnectionStatus.ACTIVE;
import static cs.bilkent.joker.engine.pipeline.UpstreamConnectionStatus.CLOSED;
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

    private final int outputPortCount = 1;

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
                                               mock( Supplier.class ), mock( PipelineReplicaMeter.class ),
                                               new InvocationContextImpl() );

        when( operatorDef.id() ).thenReturn( "op1" );
        when( operatorDef.outputPortCount() ).thenReturn( outputPortCount );
        when( operatorDef.createOperator() ).thenReturn( operator );
        when( drainerPool.acquire( anyObject() ) ).thenReturn( new NopDrainer() );
    }

    @Test
    public void shouldInitializeOperatorReplicaWhenOperatorInitializationSucceeds_ScheduleWhenTuplesAvailable_ANY_PORT_SINGLE ()
    {
        final int inputPortCount = 1;
        final ScheduleWhenTuplesAvailable schedulingStrategy = scheduleWhenTuplesAvailableOnAny( inputPortCount, 1, 0 );
        shouldInitializeOperatorSuccessfully( inputPortCount, schedulingStrategy, newUpstreamContextInstance( 0, inputPortCount, ACTIVE ) );
    }

    @Test
    public void shouldInitializeOperatorReplicaWhenOperatorInitializationSucceeds_ScheduleWhenTuplesAvailable_ANY_PORT_MULTI ()
    {
        final int inputPortCount = 2;
        final ScheduleWhenTuplesAvailable schedulingStrategy = scheduleWhenTuplesAvailableOnAny( inputPortCount, 1, 1 );
        shouldInitializeOperatorSuccessfully( inputPortCount, schedulingStrategy, newUpstreamContextInstance( 0, inputPortCount, ACTIVE ) );
    }

    @Test
    public void shouldInitializeOperatorReplicaWhenOperatorInitializationSucceeds_ScheduleWhenTuplesAvailable_ANY_PORT_MULTI_portClosed ()
    {
        final int inputPortCount = 2;
        final ScheduleWhenTuplesAvailable schedulingStrategy = scheduleWhenTuplesAvailableOnAny( inputPortCount, 1, 0, 1 );
        shouldInitializeOperatorSuccessfully( inputPortCount,
                                              schedulingStrategy,
                                              new UpstreamContext( 0, new UpstreamConnectionStatus[] { CLOSED, ACTIVE } ) );
    }

    @Test
    public void shouldInitializeOperatorReplicaWhenOperatorInitializationSucceeds_ScheduleWhenTuplesAvailable_ALL_PORTS ()
    {
        final int inputPortCount = 2;
        final ScheduleWhenTuplesAvailable schedulingStrategy = scheduleWhenTuplesAvailableOnAll( AT_LEAST, inputPortCount, 1, 0, 1 );
        shouldInitializeOperatorSuccessfully( inputPortCount, schedulingStrategy, newUpstreamContextInstance( 0, inputPortCount, ACTIVE ) );
    }

    @Test
    public void shouldInitializeOperatorReplicaWhenOperatorInitializationSucceeds_ScheduleWhenAvailable ()
    {
        final int inputPortCount = 0;
        shouldInitializeOperatorSuccessfully( inputPortCount,
                                              ScheduleWhenAvailable.INSTANCE,
                                              newUpstreamContextInstance( 0, inputPortCount, ACTIVE ) );
    }

    public void shouldInitializeOperatorSuccessfully ( final int inputPortCount,
                                                       final SchedulingStrategy schedulingStrategy,
                                                       final UpstreamContext upstreamContext )
    {
        when( operatorDef.inputPortCount() ).thenReturn( inputPortCount );
        validUpstreamContext = newUpstreamContextInstance( 0, inputPortCount, ACTIVE );

        when( operator.init( any( InitializationContext.class ) ) ).thenReturn( schedulingStrategy );

        operatorReplica.init( upstreamContext );

        assertThat( operatorReplica.getStatus(), equalTo( RUNNING ) );
        assertThat( operatorReplica.getInitialSchedulingStrategy(), equalTo( schedulingStrategy ) );
        assertThat( operatorReplica.getSchedulingStrategy(), equalTo( schedulingStrategy ) );
        assertThat( operatorReplica.getUpstreamContext(), equalTo( upstreamContext ) );
        assertThat( operatorReplica.getSelfUpstreamContext(), equalTo( newUpstreamContextInstance( 0, outputPortCount, ACTIVE ) ) );

        final UpstreamContext selfUpstreamContext = operatorReplica.getSelfUpstreamContext();
        assertThat( selfUpstreamContext, equalTo( newUpstreamContextInstance( 0, outputPortCount, ACTIVE ) ) );

        verify( drainerPool ).acquire( schedulingStrategy );
    }

    @Test
    public void shouldFailWhenOperatorInitializationReturnsInvalidSchedulingStrategy ()
    {
        when( operator.init( any( InitializationContext.class ) ) ).thenReturn( ScheduleNever.INSTANCE );

        try
        {
            operatorReplica.init( newUpstreamContextInstance( 0, 1, ACTIVE ) );
            fail();
        }
        catch ( InitializationException expected )
        {
            System.out.println( expected );
            assertFailedInitialization();
        }
    }

    @Test
    public void shouldFailWhenOperatorInitializationReturnsInvalidSchedulingStrategy_ScheduleWhenAvailable_nonZeroInputPorts ()
    {
        shouldFailToInitializeOperator( 1, ScheduleWhenAvailable.INSTANCE, newUpstreamContextInstance( 0, 1, ACTIVE ) );
    }

    @Test
    public void shouldFailWhenOperatorInitializationReturnsInvalidSchedulingStrategy_ScheduleWhenAvailable_UpstreamContextShutdown ()
    {
        shouldFailToInitializeOperator( 0, ScheduleWhenAvailable.INSTANCE, newUpstreamContextInstance( 1, 0, ACTIVE ) );
    }

    @Test
    public void shouldFailWhenOperatorInitializationReturnsInvalidSchedulingStrategy_ScheduleWhenTuplesAvailable_UpstreamContextShutdown ()
    {
        final int inputPortCount = 1;
        shouldFailToInitializeOperator( inputPortCount,
                                        scheduleWhenTuplesAvailableOnAny( inputPortCount, 1, 0 ),
                                        newUpstreamContextInstance( 1, 1, CLOSED ) );
    }

    @Test
    public void shouldFailWhenOperatorInitializationReturnsInvalidSchedulingStrategy_ScheduleWhenTuplesAvailable_ALL_PORTS_Shutdown ()
    {
        final int inputPortCount = 2;
        shouldFailToInitializeOperator( inputPortCount,
                                        scheduleWhenTuplesAvailableOnAll( AT_LEAST, inputPortCount, 1, 0, 1 ),
                                        new UpstreamContext( 1, new UpstreamConnectionStatus[] { ACTIVE, CLOSED } ) );
    }

    @Test
    public void shouldNotFailWhenOperatorInitializationReturnsSchedulingStrategy_withLessPortsThanUpstreamContext ()
    {
        final int inputPortCount = 1;
        shouldInitializeOperatorSuccessfully( inputPortCount,
                                              scheduleWhenTuplesAvailableOnAny( inputPortCount, 2, 0 ),
                                              newUpstreamContextInstance( 1, 2, ACTIVE ) );
    }

    @Test
    public void shouldFailWhenOperatorInitializationReturnsSchedulingStrategy_withMorePortsThanUpstreamContext ()
    {
        final int inputPortCount = 1;
        shouldInitializeOperatorSuccessfully( inputPortCount,
                                              scheduleWhenTuplesAvailableOnAny( inputPortCount + 1, 2, 0 ),
                                              newUpstreamContextInstance( 1, 1, ACTIVE ) );
    }

    private void shouldFailToInitializeOperator ( final int inputPortCount,
                                                  final SchedulingStrategy schedulingStrategy,
                                                  final UpstreamContext upstreamContext )
    {
        when( operatorDef.inputPortCount() ).thenReturn( inputPortCount );
        when( operator.init( any( InitializationContext.class ) ) ).thenReturn( schedulingStrategy );

        try
        {
            operatorReplica.init( upstreamContext );
            fail();
        }
        catch ( InitializationException expected )
        {
            System.out.println( expected );
            assertFailedInitialization();
        }
    }

    @Test
    public void shouldSetStatusWhenOperatorInitializationFails ()
    {

        final RuntimeException exception = new RuntimeException();
        when( operator.init( anyObject() ) ).thenThrow( exception );

        try
        {
            operatorReplica.init( validUpstreamContext );
            fail();
        }
        catch ( InitializationException expected )
        {
            System.out.println( expected );
            assertFailedInitialization();
        }

    }

    private void assertFailedInitialization ()
    {
        assertThat( operatorReplica.getStatus(), equalTo( INITIALIZATION_FAILED ) );
        assertThat( operatorReplica.getSelfUpstreamContext(), equalTo( newUpstreamContextInstance( 0, 1, CLOSED ) ) );
    }

    static UpstreamContext newUpstreamContextInstance ( final int version, final int portCount, final UpstreamConnectionStatus status )
    {
        final UpstreamConnectionStatus[] upstreamConnectionStatuses = new UpstreamConnectionStatus[ portCount ];
        Arrays.fill( upstreamConnectionStatuses, status );

        return new UpstreamContext( version, upstreamConnectionStatuses );
    }

}
