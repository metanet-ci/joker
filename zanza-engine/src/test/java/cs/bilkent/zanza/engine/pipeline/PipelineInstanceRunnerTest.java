package cs.bilkent.zanza.engine.pipeline;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static cs.bilkent.zanza.engine.TestUtils.assertTrueEventually;
import cs.bilkent.zanza.engine.config.ZanzaConfig;
import cs.bilkent.zanza.engine.coordinator.CoordinatorHandle;
import static cs.bilkent.zanza.engine.pipeline.PipelineInstanceRunnerStatus.COMPLETED;
import static cs.bilkent.zanza.engine.pipeline.PipelineInstanceRunnerStatus.PAUSED;
import static cs.bilkent.zanza.engine.pipeline.PipelineInstanceRunnerStatus.RUNNING;
import cs.bilkent.zanza.operator.Tuple;
import cs.bilkent.zanza.operator.impl.TuplesImpl;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PipelineInstanceRunnerTest
{

    private PipelineInstance pipeline;

    private CoordinatorHandle coordinator;

    private DownstreamTupleSender downstreamTupleSender;

    private PipelineInstanceRunner runner;

    private Thread thread;

    @Before
    public void init () throws Exception
    {
        pipeline = mock( PipelineInstance.class );
        coordinator = mock( CoordinatorHandle.class );
        downstreamTupleSender = mock( DownstreamTupleSender.class );
        runner = new PipelineInstanceRunner( pipeline );
        runner.init( new ZanzaConfig() );
        runner.setCoordinator( coordinator );
        runner.setDownstreamTupleSender( downstreamTupleSender );
        thread = new Thread( runner );
    }

    @After
    public void after () throws InterruptedException
    {
        try
        {
            stopAndGetFuture().get();
        }
        catch ( InterruptedException e )
        {
            e.printStackTrace();
        }
        catch ( ExecutionException expected )
        {

        }

        thread.join();
    }

    private CompletableFuture<Void> stopAndGetFuture ()
    {
        final CompletableFuture<Void> future = runner.stop();
        reset( pipeline );
        return future;
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

        assertTrueEventually( () -> {
            assertEquals( PAUSED, runner.getStatus() );
        } );
    }

    @Test
    public void shouldPauseWhenAlreadyPaused () throws ExecutionException, InterruptedException
    {
        startRunner();

        runner.pause().get();

        assertTrueEventually( () -> {
            assertEquals( PAUSED, runner.getStatus() );
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

        stopAndGetFuture().get();

        assertTrueEventually( () -> assertEquals( runner.getStatus(), COMPLETED ) );

        runner.pause().get();
    }

    @Test( expected = ExecutionException.class )
    public void shouldNotResumeAfterStopped () throws ExecutionException, InterruptedException
    {
        startRunner();

        stopAndGetFuture().get();

        assertTrueEventually( () -> assertEquals( runner.getStatus(), COMPLETED ) );

        runner.resume().get();
    }

    @Test
    public void shouldStopWhileRunning () throws ExecutionException, InterruptedException
    {
        final CountDownLatch invocationStartLatch = new CountDownLatch( 1 );
        final CountDownLatch invocationDoneLatch = new CountDownLatch( 1 );
        final TuplesImpl output = new TuplesImpl( 1 );


        when( pipeline.invoke() ).thenAnswer( invocation -> {
            invocationStartLatch.countDown();
            invocationDoneLatch.await();
            return output;
        } );

        startRunner();

        final CompletableFuture<Void> future = stopAndGetFuture();
        invocationStartLatch.await();
        invocationDoneLatch.countDown();
        future.get();
        assertTrueEventually( () -> assertEquals( runner.getStatus(), COMPLETED ) );
        verify( downstreamTupleSender ).send( null, output );
    }

    @Test
    public void shouldStopWhenPaused () throws ExecutionException, InterruptedException
    {
        startRunner();

        runner.pause().get();

        assertTrueEventually( () -> assertEquals( runner.getStatus(), PAUSED ) );

        stopAndGetFuture().get();
    }

    @Test
    public void shouldInvokeStopMultipleTimes () throws ExecutionException, InterruptedException
    {
        startRunner();

        stopAndGetFuture().get();
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
        final TuplesImpl output1 = TuplesImpl.newInstanceWithSinglePort( new Tuple( "k1", "v1" ) );
        final TuplesImpl output2 = TuplesImpl.newInstanceWithSinglePort( new Tuple( "k2", "v2" ) );

        when( pipeline.invoke() ).thenReturn( output1, output2 );

        startRunner();

        sleepUninterruptibly( 1, SECONDS );
        reset( pipeline );

        assertTrueEventually( () -> assertEquals( runner.getStatus(), COMPLETED ), 10 );
        verify( downstreamTupleSender, atLeastOnce() ).send( null, output1 );
        verify( downstreamTupleSender, atLeastOnce() ).send( null, output2 );
    }

    private void startRunner ()
    {
        when( pipeline.isInvokableOperatorAvailable() ).thenReturn( true );

        thread.start();

        assertTrueEventually( () -> {
            assertEquals( RUNNING, runner.getStatus() );
        } );
    }

}
