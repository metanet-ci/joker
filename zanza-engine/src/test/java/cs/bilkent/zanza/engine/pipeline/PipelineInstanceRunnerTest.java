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

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static cs.bilkent.zanza.engine.TestUtils.assertTrueEventually;
import cs.bilkent.zanza.engine.config.ZanzaConfig;
import static cs.bilkent.zanza.engine.pipeline.PipelineInstanceRunnerStatus.COMPLETED;
import static cs.bilkent.zanza.engine.pipeline.PipelineInstanceRunnerStatus.PAUSED;
import static cs.bilkent.zanza.engine.pipeline.PipelineInstanceRunnerStatus.RUNNING;
import static cs.bilkent.zanza.engine.pipeline.UpstreamConnectionStatus.ACTIVE;
import static cs.bilkent.zanza.engine.pipeline.UpstreamConnectionStatus.CLOSED;
import cs.bilkent.zanza.engine.supervisor.Supervisor;
import cs.bilkent.zanza.flow.OperatorDefinition;
import cs.bilkent.zanza.operator.Tuple;
import cs.bilkent.zanza.operator.impl.TuplesImpl;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.class )
public class PipelineInstanceRunnerTest
{

    private String operatorId = "op1";

    @Mock
    private OperatorDefinition operatorDefinition;

    @Mock
    private OperatorInstance operator;

    @Mock
    private Supervisor supervisor;

    @Mock
    private SupervisorNotifier supervisorNotifier;

    @Mock
    private DownstreamTupleSender downstreamTupleSender;

    private PipelineInstance pipeline;

    private PipelineInstanceRunner runner;

    private Thread thread;

    @Before
    public void init () throws Exception
    {
        final PipelineInstanceId id = new PipelineInstanceId( 0, 0, 0 );
        pipeline = new PipelineInstance( id, new OperatorInstance[] { operator } );
        runner = new PipelineInstanceRunner( pipeline );
        runner.setSupervisor( supervisor );
        runner.setDownstreamTupleSender( downstreamTupleSender );

        when( operator.getOperatorDefinition() ).thenReturn( operatorDefinition );
        when( operatorDefinition.id() ).thenReturn( operatorId );

        when( supervisor.getUpstreamContext( id ) ).thenReturn( new UpstreamContext( 0, new UpstreamConnectionStatus[] { ACTIVE } ) );
        runner.init( new ZanzaConfig(), supervisorNotifier );

        thread = new Thread( runner );

        when( operator.isInvokable() ).thenReturn( true );
    }

    @After
    public void after () throws InterruptedException
    {
        try
        {
            updatePipelineUpstreamContextForCompletion().get();
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

    private CompletableFuture<Void> updatePipelineUpstreamContextForCompletion ()
    {
        when( supervisorNotifier.isPipelineCompleted() ).thenReturn( true );
        final CompletableFuture<Void> future = runner.updatePipelineUpstreamContext();
        return future;
    }

    @Test
    public void shouldStartRunning () throws ExecutionException, InterruptedException
    {
        startRunner();
    }

    @Test
    public void shouldPauseWhileRunning () throws ExecutionException, InterruptedException
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
    public void shouldNotPauseAfterCompleted () throws ExecutionException, InterruptedException
    {
        startRunner();

        updatePipelineUpstreamContextForCompletion();

        assertTrueEventually( () -> assertEquals( runner.getStatus(), COMPLETED ) );

        runner.pause().get();
    }

    @Test( expected = ExecutionException.class )
    public void shouldNotResumeAfterCompleted () throws ExecutionException, InterruptedException
    {
        startRunner();

        updatePipelineUpstreamContextForCompletion();

        assertTrueEventually( () -> assertEquals( runner.getStatus(), COMPLETED ) );

        runner.resume().get();
    }

    @Test
    public void shouldCompleteWhileRunning () throws ExecutionException, InterruptedException
    {
        final CountDownLatch invocationStartLatch = new CountDownLatch( 1 );
        final CountDownLatch invocationDoneLatch = new CountDownLatch( 1 );
        final TuplesImpl output = new TuplesImpl( 1 );

        when( operator.invoke( anyObject(), anyObject() ) ).thenAnswer( invocation -> {
            invocationStartLatch.countDown();
            invocationDoneLatch.await();
            return output;
        } );

        startRunner();

        final CompletableFuture<Void> future = updatePipelineUpstreamContextForCompletion();
        invocationStartLatch.await();
        invocationDoneLatch.countDown();
        future.get();
        assertTrueEventually( () -> assertEquals( runner.getStatus(), COMPLETED ) );
        verify( downstreamTupleSender ).send( output );
    }

    @Test
    public void shouldUpdateUpstreamContextWhenPaused () throws ExecutionException, InterruptedException
    {
        startRunner();

        runner.pause().get();

        assertTrueEventually( () -> assertEquals( runner.getStatus(), PAUSED ) );

        final UpstreamContext upstreamContext = new UpstreamContext( 1, new UpstreamConnectionStatus[] { CLOSED } );
        when( supervisor.getUpstreamContext( pipeline.id() ) ).thenReturn( upstreamContext );

        updatePipelineUpstreamContextForCompletion().get();
        assertThat( runner.getPipelineUpstreamContext(), equalTo( upstreamContext ) );
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

        when( operator.invoke( anyObject(), anyObject() ) ).thenReturn( output1, output2 );

        startRunner();

        sleepUninterruptibly( 1, SECONDS );
        when( supervisorNotifier.isPipelineCompleted() ).thenReturn( true );

        assertTrueEventually( () -> assertEquals( runner.getStatus(), COMPLETED ), 10 );
        verify( downstreamTupleSender, atLeastOnce() ).send( output1 );
        verify( downstreamTupleSender, atLeastOnce() ).send( output2 );
    }

    private void startRunner ()
    {
        thread.start();

        assertTrueEventually( () -> {
            assertEquals( RUNNING, runner.getStatus() );
        } );
    }

}
