package cs.bilkent.zanza.engine.pipeline;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import static cs.bilkent.zanza.engine.pipeline.OperatorInstanceStatus.COMPLETED;
import static cs.bilkent.zanza.engine.pipeline.UpstreamConnectionStatus.ACTIVE;
import cs.bilkent.zanza.operator.impl.TuplesImpl;
import static cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.scheduleWhenTuplesAvailableOnDefaultPort;
import cs.bilkent.zanza.operator.scheduling.SchedulingStrategy;
import static org.junit.Assert.assertNull;

@RunWith( MockitoJUnitRunner.class )
public class OperatorInstanceCompletedStatusTest extends AbstractOperatorInstanceInvocationTest
{

    @Test
    public void shouldNotInvokeWhenOperatorInstanceStatusIsCompleted ()
    {
        final int inputPortCount = 1, outputPortCount = 1;
        final SchedulingStrategy initializationStrategy = scheduleWhenTuplesAvailableOnDefaultPort( 1 );
        initializeOperatorInstance( inputPortCount, outputPortCount, initializationStrategy );
        moveOperatorInstanceToStatus( COMPLETED );

        assertNull( operatorInstance.invoke( new TuplesImpl( inputPortCount ),
                                             OperatorInstanceInitializationTest.newUpstreamContextInstance( 0, inputPortCount, ACTIVE ) ) );
    }

}
