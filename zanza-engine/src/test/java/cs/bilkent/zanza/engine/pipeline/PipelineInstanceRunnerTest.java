package cs.bilkent.zanza.engine.pipeline;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static cs.bilkent.zanza.engine.TestUtils.assertEventually;
import cs.bilkent.zanza.engine.coordinator.CoordinatorHandle;
import static cs.bilkent.zanza.engine.pipeline.PipelineInstance.NO_INVOKABLE_INDEX;
import static cs.bilkent.zanza.engine.pipeline.PipelineInstanceRunnerStatus.COMPLETED;
import static cs.bilkent.zanza.engine.pipeline.PipelineInstanceRunnerStatus.PAUSED;
import static cs.bilkent.zanza.engine.pipeline.PipelineInstanceRunnerStatus.RUNNING;
import cs.bilkent.zanza.operator.PortsToTuples;
import cs.bilkent.zanza.operator.Tuple;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.class )
public class PipelineInstanceRunnerTest
{

    @Mock
    private PipelineInstance pipeline;

    @Mock
    private CoordinatorHandle coordinator;

    @Mock
    private DownstreamTupleSender downstreamTupleSender;

    private PipelineInstanceRunner runner;

    private Thread thread;

    @Before
    public void init () throws Exception
    {
        runner = new PipelineInstanceRunner( pipeline );
        runner.setCoordinator( coordinator );
        runner.setDownstreamTupleSender( downstreamTupleSender );
        thread = new Thread( runner );
    }

    @After
    public void after ()
    {
        try
        {
            runner.stop().get();
        }
        catch ( Exception expected )
        {

        }
    }

    @Test
    public void shouldSetStatusWhenStartedRunning () throws ExecutionException, InterruptedException
    {
        startRunner();
    }

    @Test
    public void shouldSetStatusWhenPaused () throws ExecutionException, InterruptedException
    {
        startRunner();

        runner.pause().get();

        assertEventually( () -> {
            assertEquals( PAUSED, runner.status() );
        } );
    }

    @Test
    public void shouldPauseWhenAlreadyPaused () throws ExecutionException, InterruptedException
    {
        startRunner();

        runner.pause().get();

        assertEventually( () -> {
            assertEquals( PAUSED, runner.status() );
        } );

        runner.pause().get();
    }

    @Test
    public void shouldNotPauseBeforeStartedRunning () throws InterruptedException
    {
        try
        {
            runner.pause().get();
            fail();
        }
        catch ( ExecutionException e )
        {
            assertTrue( e.getCause() instanceof IllegalStateException );
        }
    }

    @Test( expected = ExecutionException.class )
    public void shouldNotPauseAfterStopped () throws ExecutionException, InterruptedException
    {
        startRunner();

        runner.stop().get();

        assertEventually( () -> assertEquals( runner.status(), COMPLETED ) );

        runner.pause().get();
    }

    @Test( expected = ExecutionException.class )
    public void shouldNotResumeAfterStopped () throws ExecutionException, InterruptedException
    {
        startRunner();

        runner.stop().get();

        assertEventually( () -> assertEquals( runner.status(), COMPLETED ) );

        runner.resume().get();
    }

    @Test
    public void shouldStopWhileRunning () throws ExecutionException, InterruptedException
    {
        final CountDownLatch latch = new CountDownLatch( 1 );
        final PortsToTuples output = new PortsToTuples();

        when( pipeline.invoke() ).thenAnswer( invocation -> {
            latch.await();
            return output;
        } );

        startRunner();

        final CompletableFuture<Void> future = runner.stop();
        latch.countDown();

        future.get();
        assertEventually( () -> assertEquals( runner.status(), COMPLETED ) );
        verify( downstreamTupleSender ).send( null, output );
    }

    @Test
    public void shouldStopWhenPaused () throws ExecutionException, InterruptedException
    {
        startRunner();

        runner.pause().get();

        assertEventually( () -> assertEquals( runner.status(), PAUSED ) );

        runner.stop().get();
    }

    @Test
    public void shouldInvokeStopMultipleTimes () throws ExecutionException, InterruptedException
    {
        startRunner();

        runner.stop().get();
        runner.stop().get();
    }

    @Test
    public void shouldResumeWhileRunning () throws ExecutionException, InterruptedException
    {
        startRunner();

        runner.resume().get();
    }

    @Test
    public void shouldNotResumeBeforeStartedRunning () throws InterruptedException
    {
        try
        {
            runner.pause().get();
            fail();
        }
        catch ( ExecutionException e )
        {
            assertTrue( e.getCause() instanceof IllegalStateException );
        }
    }

    @Test
    public void shouldCompleteRunningAfterPipelineCompletesItself ()
    {
        final PortsToTuples output1 = new PortsToTuples( new Tuple( "k1", "v1" ) );
        final PortsToTuples output2 = new PortsToTuples( new Tuple( "k2", "v2" ) );

        when( pipeline.invoke() ).thenReturn( output1, output2 );
        when( pipeline.currentHighestInvokableIndex() ).thenReturn( 0, 0, NO_INVOKABLE_INDEX );

        thread.start();

        assertEventually( () -> assertEquals( runner.status(), COMPLETED ), 10 );
        verify( downstreamTupleSender ).send( null, output1 );
        verify( downstreamTupleSender ).send( null, output2 );
    }

    private void startRunner ()
    {
        thread.start();

        assertEventually( () -> {
            assertEquals( RUNNING, runner.status() );
        } );
    }

}
