package cs.bilkent.zanza.engine.pipeline;


import java.util.Arrays;
import java.util.function.Supplier;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import cs.bilkent.zanza.engine.exception.InitializationException;
import cs.bilkent.zanza.engine.kvstore.KVStoreContext;
import static cs.bilkent.zanza.engine.pipeline.OperatorReplicaStatus.INITIALIZATION_FAILED;
import static cs.bilkent.zanza.engine.pipeline.OperatorReplicaStatus.RUNNING;
import static cs.bilkent.zanza.engine.pipeline.UpstreamConnectionStatus.ACTIVE;
import static cs.bilkent.zanza.engine.pipeline.UpstreamConnectionStatus.CLOSED;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueContext;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueDrainerPool;
import cs.bilkent.zanza.flow.OperatorDef;
import cs.bilkent.zanza.operator.InitializationContext;
import cs.bilkent.zanza.operator.Operator;
import cs.bilkent.zanza.operator.impl.InvocationContextImpl;
import cs.bilkent.zanza.operator.scheduling.ScheduleNever;
import cs.bilkent.zanza.operator.scheduling.ScheduleWhenAvailable;
import cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable;
import static cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.AT_LEAST;
import static cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.scheduleWhenTuplesAvailableOnAll;
import static cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.scheduleWhenTuplesAvailableOnAny;
import cs.bilkent.zanza.operator.scheduling.SchedulingStrategy;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.class )
public class OperatorReplicaInitializationTest
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
        operatorReplica = new OperatorReplica( new PipelineReplicaId( new PipelineId( 0, 0 ), 0 ), operatorDef,
                                               mock( TupleQueueContext.class ),
                                               mock( KVStoreContext.class ),
                                               drainerPool,
                                               mock( Supplier.class ),
                                               new InvocationContextImpl() );

        when( operatorDef.id() ).thenReturn( "op1" );
        when( operatorDef.outputPortCount() ).thenReturn( outputPortCount );
        when( operatorDef.createOperator() ).thenReturn( operator );
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

        operatorReplica.init( upstreamContext, null );

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
            operatorReplica.init( newUpstreamContextInstance( 0, 1, ACTIVE ), null );
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
    public void shouldFailWhenOperatorInitializationReturnsInvalidSchedulingStrategy_incompatibleUpstreamContext ()
    {
        final int inputPortCount = 1;
        shouldFailToInitializeOperator( inputPortCount,
                                        scheduleWhenTuplesAvailableOnAny( inputPortCount, 2, 0 ),
                                        newUpstreamContextInstance( 1, 2, ACTIVE ) );
    }

    @Test
    public void shouldFailWhenOperatorInitializationReturnsInvalidSchedulingStrategy_incompatibleSchedulingStrategy ()
    {
        final int inputPortCount = 1;
        shouldFailToInitializeOperator( inputPortCount,
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
            operatorReplica.init( upstreamContext, null );
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
            operatorReplica.init( validUpstreamContext, null );
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

    public static UpstreamContext withUpstreamConnectionStatus ( final UpstreamContext upstreamContext,
                                                                 final int portIndex,
                                                                 final UpstreamConnectionStatus newStatus )
    {
        final int portCount = upstreamContext.getPortCount();
        final UpstreamConnectionStatus[] statuses = new UpstreamConnectionStatus[ portCount ];
        for ( int i = 0; i < portCount; i++ )
        {
            statuses[ i ] = upstreamContext.getUpstreamConnectionStatus( i );
        }
        statuses[ portIndex ] = newStatus;
        return new UpstreamContext( upstreamContext.getVersion() + 1, statuses );
    }

}
