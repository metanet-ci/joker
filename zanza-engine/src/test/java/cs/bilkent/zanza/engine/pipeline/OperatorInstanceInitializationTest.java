package cs.bilkent.zanza.engine.pipeline;


import java.util.Arrays;
import java.util.function.Supplier;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import cs.bilkent.zanza.engine.config.ZanzaConfig;
import cs.bilkent.zanza.engine.exception.InitializationException;
import cs.bilkent.zanza.engine.kvstore.KVStoreProvider;
import static cs.bilkent.zanza.engine.pipeline.OperatorInstanceStatus.INITIALIZATION_FAILED;
import static cs.bilkent.zanza.engine.pipeline.OperatorInstanceStatus.RUNNING;
import static cs.bilkent.zanza.engine.pipeline.UpstreamConnectionStatus.ACTIVE;
import static cs.bilkent.zanza.engine.pipeline.UpstreamConnectionStatus.CLOSED;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueContext;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueDrainerPool;
import cs.bilkent.zanza.flow.OperatorDefinition;
import cs.bilkent.zanza.operator.InitializationContext;
import cs.bilkent.zanza.operator.Operator;
import cs.bilkent.zanza.operator.impl.InvocationContextImpl;
import cs.bilkent.zanza.operator.scheduling.ScheduleNever;
import cs.bilkent.zanza.operator.scheduling.ScheduleWhenAvailable;
import static cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.scheduleWhenTuplesAvailableOnAll;
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
public class OperatorInstanceInitializationTest
{
    @Mock
    private Operator operator;

    @Mock
    private OperatorDefinition operatorDefinition;

    @Mock
    private TupleQueueDrainerPool drainerPool;

    private final int inputOutputPortCount = 1;

    private OperatorInstance operatorInstance;

    private UpstreamContext validUpstreamContext;

    @Before
    public void before () throws InstantiationException, IllegalAccessException
    {
        operatorInstance = new OperatorInstance( new PipelineInstanceId( 0, 0, 0 ),
                                                 operatorDefinition,
                                                 mock( TupleQueueContext.class ),
                                                 mock( KVStoreProvider.class ),
                                                 drainerPool,
                                                 mock( Supplier.class ),
                                                 new InvocationContextImpl() );

        when( operatorDefinition.id() ).thenReturn( "op1" );
        when( operatorDefinition.inputPortCount() ).thenReturn( inputOutputPortCount );
        when( operatorDefinition.outputPortCount() ).thenReturn( inputOutputPortCount );
        when( operatorDefinition.createOperator() ).thenReturn( operator );

        validUpstreamContext = newUpstreamContextInstance( 0, inputOutputPortCount, ACTIVE );
    }

    @Test
    public void shouldInitializeOperatorInstanceWhenOperatorInitializationSucceeds ()
    {
        when( operator.init( any( InitializationContext.class ) ) ).thenReturn( ScheduleWhenAvailable.INSTANCE );

        operatorInstance.init( new ZanzaConfig(), validUpstreamContext );

        assertThat( operatorInstance.getStatus(), equalTo( RUNNING ) );
        assertThat( operatorInstance.getSchedulingStrategy(), equalTo( ScheduleWhenAvailable.INSTANCE ) );
        assertThat( operatorInstance.getUpstreamContext(), equalTo( validUpstreamContext ) );

        final UpstreamContext selfUpstreamContext = operatorInstance.getSelfUpstreamContext();
        assertThat( selfUpstreamContext, equalTo( validUpstreamContext ) );

        verify( drainerPool ).acquire( ScheduleWhenAvailable.INSTANCE );
    }

    @Test
    public void shouldSetStatusWhenOperatorInitializationFails ()
    {

        final RuntimeException exception = new RuntimeException();
        when( operator.init( anyObject() ) ).thenThrow( exception );

        try
        {
            operatorInstance.init( new ZanzaConfig(), validUpstreamContext );
            fail();
        }
        catch ( InitializationException expected )
        {
            System.out.println( expected );
            assertFailedInitialization();
        }

    }

    @Test
    public void shouldFailWhenOperatorInitializationReturnsInvalidSchedulingStrategy ()
    {
        when( operator.init( any( InitializationContext.class ) ) ).thenReturn( ScheduleNever.INSTANCE );

        try
        {
            operatorInstance.init( new ZanzaConfig(), validUpstreamContext );
            fail();
        }
        catch ( InitializationException expected )
        {
            System.out.println( expected );
            assertFailedInitialization();
        }
    }

    @Test
    public void shouldFailWhenOperatorInitializationReturnedSchedulingStrategyDoesntMatchUpstreamContext ()
    {
        final SchedulingStrategy schedulingStrategy = scheduleWhenTuplesAvailableOnAll( 2, 1, 0, 1 );
        when( operator.init( any( InitializationContext.class ) ) ).thenReturn( schedulingStrategy );

        try
        {
            operatorInstance.init( new ZanzaConfig(), validUpstreamContext );
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
        assertThat( operatorInstance.getStatus(), equalTo( INITIALIZATION_FAILED ) );
        assertThat( operatorInstance.getSelfUpstreamContext(), equalTo( newUpstreamContextInstance( 0, 1, CLOSED ) ) );
    }

    static UpstreamContext newUpstreamContextInstance ( final int version, final int portCount, final UpstreamConnectionStatus status )
    {
        final UpstreamConnectionStatus[] upstreamConnectionStatuses = new UpstreamConnectionStatus[ portCount ];
        Arrays.fill( upstreamConnectionStatuses, status );

        return new UpstreamContext( version, upstreamConnectionStatuses );
    }

}
