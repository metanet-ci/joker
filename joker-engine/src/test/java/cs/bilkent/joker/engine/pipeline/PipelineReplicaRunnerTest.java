package cs.bilkent.joker.engine.pipeline;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import cs.bilkent.joker.engine.config.JokerConfig;
import cs.bilkent.joker.engine.metric.PipelineReplicaMeter;
import static cs.bilkent.joker.engine.pipeline.PipelineReplicaRunner.PipelineReplicaRunnerStatus.COMPLETED;
import static cs.bilkent.joker.engine.pipeline.PipelineReplicaRunner.PipelineReplicaRunnerStatus.PAUSED;
import static cs.bilkent.joker.engine.pipeline.UpstreamCtx.createInitialClosedUpstreamCtx;
import cs.bilkent.joker.engine.supervisor.Supervisor;
import cs.bilkent.joker.engine.tuplequeue.OperatorQueue;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.impl.TuplesImpl;
import cs.bilkent.joker.operator.scheduling.SchedulingStrategy;
import cs.bilkent.joker.test.AbstractJokerTest;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.class )
public class PipelineReplicaRunnerTest extends AbstractJokerTest
{

    @Mock
    private OperatorDef operatorDef;

    @Mock
    private OperatorReplica operator;

    @Mock
    private Supervisor supervisor;

    @Mock
    private SchedulingStrategy schedulingStrategy;

    @Mock
    private UpstreamCtx upstreamCtx;

    @Mock
    private DownstreamCollector downstreamCollector;

    private PipelineReplica pipeline;

    private PipelineReplicaRunner runner;

    private Thread thread;

    private final int inputOutputPortCount = 1;

    private final PipelineReplicaId id = new PipelineReplicaId( 0, 0, 0 );

    @Before
    public void init ()
    {
        when( supervisor.getUpstreamCtx( id ) ).thenReturn( upstreamCtx );
        when( supervisor.getDownstreamCollector( id ) ).thenReturn( mock( DownstreamCollector.class ) );
        when( operator.getOperatorDef( 0 ) ).thenReturn( operatorDef );
        when( operatorDef.getId() ).thenReturn( "op1" );
        when( operatorDef.getInputPortCount() ).thenReturn( inputOutputPortCount );
        when( operatorDef.getOutputPortCount() ).thenReturn( inputOutputPortCount );

        when( operator.init( new UpstreamCtx[] { upstreamCtx }, null ) ).thenReturn( new SchedulingStrategy[] { schedulingStrategy } );

        final JokerConfig config = new JokerConfig();
        pipeline = new PipelineReplica( id, new OperatorReplica[] { operator }, mock( OperatorQueue.class ),
                                        new PipelineReplicaMeter( config.getMetricManagerConfig().getTickMask(), id, operatorDef ) );

        final SchedulingStrategy[][] schedulingStrategies = new SchedulingStrategy[ 1 ][ 1 ];
        schedulingStrategies[ 0 ][ 0 ] = schedulingStrategy;
        final UpstreamCtx[][] upstreamCtxes = new UpstreamCtx[ 1 ][ 1 ];
        upstreamCtxes[ 0 ][ 0 ] = upstreamCtx;

        pipeline.init( schedulingStrategies, upstreamCtxes );
        runner = new PipelineReplicaRunner( config, pipeline, supervisor, downstreamCollector );

        thread = new Thread( runner );

        when( operator.isInvokable() ).thenReturn( true );
    }

    @After
    public void after () throws InterruptedException
    {
        try
        {
            runner.stop().get();
        }
        catch ( ExecutionException | InterruptedException e )
        {
            fail( e.getMessage() );
        }

        thread.join();
    }

    @Test
    public void shouldCompletePauseWhenRunAfterwards ()
    {
        final CompletableFuture<Boolean> future = runner.pause();
        runner.resume();

        thread.start();

        assertTrue( future.isDone() );
    }

    @Test
    public void shouldPauseWhileRunning () throws ExecutionException, InterruptedException
    {
        thread.start();

        runner.pause().get();

        assertTrueEventually( () -> assertEquals( PAUSED, runner.getStatus() ) );
    }

    @Test
    public void shouldStopWhileRunning () throws ExecutionException, InterruptedException
    {
        thread.start();

        runner.stop().get();

        assertTrueEventually( () -> assertEquals( COMPLETED, runner.getStatus() ) );
    }

    @Test
    public void shouldPauseWhenAlreadyPaused () throws ExecutionException, InterruptedException
    {
        thread.start();

        runner.pause().get();

        assertTrueEventually( () -> assertEquals( PAUSED, runner.getStatus() ) );

        runner.pause().get();
    }

    @Test
    public void shouldNotPauseAfterStopped () throws InterruptedException, ExecutionException
    {
        thread.start();

        runner.stop().get();

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
        thread.start();

        pipeline.getCompletionTracker().onStatusChange( operatorDef.getId(), OperatorReplicaStatus.COMPLETED );

        assertTrueEventually( () -> assertEquals( runner.getStatus(), COMPLETED ) );

        runner.pause().get();
    }

    @Test
    public void shouldNotResumeAfterStopped () throws InterruptedException, ExecutionException
    {
        thread.start();

        runner.stop().get();

        try
        {
            runner.resume().get();
            fail();
        }
        catch ( ExecutionException e )
        {
            assertTrue( e.getCause() instanceof IllegalStateException );
        }
    }

    @Test( expected = ExecutionException.class )
    public void shouldNotResumeAfterCompleted () throws ExecutionException, InterruptedException
    {
        thread.start();

        pipeline.getCompletionTracker().onStatusChange( operatorDef.getId(), OperatorReplicaStatus.COMPLETED );

        assertTrueEventually( () -> assertEquals( runner.getStatus(), COMPLETED ) );

        runner.resume().get();
    }

    @Test
    public void shouldCompleteWhileRunning () throws InterruptedException
    {
        final CountDownLatch invocationStartLatch = new CountDownLatch( 1 );
        final CountDownLatch invocationDoneLatch = new CountDownLatch( 1 );
        final TuplesImpl output = new TuplesImpl( 1 );
        output.add( new Tuple() );

        when( operator.invoke( anyBoolean(), anyObject(), anyObject() ) ).thenAnswer( invocation -> {
            invocationStartLatch.countDown();
            invocationDoneLatch.await( 2, TimeUnit.MINUTES );
            return output;
        } );

        thread.start();

        reset( supervisor );

        pipeline.getCompletionTracker().onStatusChange( operatorDef.getId(), OperatorReplicaStatus.COMPLETED );

        invocationStartLatch.await( 2, TimeUnit.MINUTES );
        invocationDoneLatch.countDown();

        assertTrueEventually( () -> assertEquals( runner.getStatus(), COMPLETED ) );
        verify( downstreamCollector ).accept( output );
    }

    @Test
    public void shouldUpdateUpstreamContextWhenPaused () throws ExecutionException, InterruptedException
    {
        thread.start();

        runner.pause().get();

        assertTrueEventually( () -> assertEquals( runner.getStatus(), PAUSED ) );

        final UpstreamCtx upstreamCtx = createInitialClosedUpstreamCtx( 1 ).withConnectionClosed( 0 );
        when( supervisor.getUpstreamCtx( pipeline.id() ) ).thenReturn( upstreamCtx );

        final CompletableFuture<Boolean> future = runner.updatePipelineUpstreamCtx();
        assertTrue( future.get() );
        assertEquals( PAUSED, runner.getStatus() );

        assertTrueEventually( () -> verify( supervisor, times( 2 ) ).getUpstreamCtx( id ) );
        pipeline.getCompletionTracker().onStatusChange( operatorDef.getId(), OperatorReplicaStatus.COMPLETED );

        assertThat( runner.getPipelineUpstreamCtx(), equalTo( upstreamCtx ) );
    }

    @Test
    public void shouldNotUpdateUpstreamContextWhenStopped () throws ExecutionException, InterruptedException
    {
        thread.start();

        runner.stop().get();

        assertTrueEventually( () -> assertEquals( runner.getStatus(), COMPLETED ) );

        final CompletableFuture<Boolean> future = runner.updatePipelineUpstreamCtx();
        try
        {
            future.get();
            fail();
        }
        catch ( ExecutionException ignored )
        {

        }
    }

    @Test
    public void shouldNotUpdateUpstreamContextWhenCompleted () throws InterruptedException
    {
        thread.start();

        pipeline.getCompletionTracker().onStatusChange( operatorDef.getId(), OperatorReplicaStatus.COMPLETED );

        assertTrueEventually( () -> assertEquals( runner.getStatus(), COMPLETED ) );

        final CompletableFuture<Boolean> future = runner.updatePipelineUpstreamCtx();
        try
        {
            future.get();
            fail();
        }
        catch ( ExecutionException ignored )
        {

        }
    }


    @Test
    public void shouldResumeWhileRunning () throws ExecutionException, InterruptedException
    {
        thread.start();

        runner.resume().get();
    }

    @Test
    public void shouldCompleteRunningAfterPipelineCompletesItself ()
    {
        final TuplesImpl output1 = new TuplesImpl( inputOutputPortCount );
        output1.add( Tuple.of( "k1", "v1" ) );
        final TuplesImpl output2 = new TuplesImpl( inputOutputPortCount );
        output2.add( Tuple.of( "k2", "v2" ) );

        when( operator.invoke( anyBoolean(), anyObject(), anyObject() ) ).thenReturn( output1, output2 );

        thread.start();

        sleepUninterruptibly( 1, SECONDS );

        pipeline.getCompletionTracker().onStatusChange( operatorDef.getId(), OperatorReplicaStatus.COMPLETED );

        assertTrueEventually( () -> assertEquals( runner.getStatus(), COMPLETED ), 10 );
        verify( downstreamCollector, atLeastOnce() ).accept( output1 );
        verify( downstreamCollector, atLeastOnce() ).accept( output2 );
    }

    @Test
    public void shouldCompleteRunningWhenPipelineFailsDuringInvocations ()
    {
        final RuntimeException failure = new RuntimeException( "expected" );
        when( operator.invoke( anyBoolean(), anyObject(), anyObject() ) ).thenThrow( failure );

        thread.start();

        assertTrueEventually( () -> assertEquals( runner.getStatus(), COMPLETED ), 10 );
        verify( supervisor ).notifyPipelineReplicaFailed( id, failure );
    }

}
