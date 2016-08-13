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
import static cs.bilkent.joker.engine.pipeline.PipelineReplicaRunner.PipelineReplicaRunnerStatus.COMPLETED;
import static cs.bilkent.joker.engine.pipeline.PipelineReplicaRunner.PipelineReplicaRunnerStatus.PAUSED;
import static cs.bilkent.joker.engine.pipeline.UpstreamConnectionStatus.CLOSED;
import cs.bilkent.joker.engine.supervisor.Supervisor;
import cs.bilkent.joker.engine.tuplequeue.TupleQueueContext;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.impl.TuplesImpl;
import cs.bilkent.joker.testutils.AbstractJokerTest;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
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
    private SupervisorNotifier supervisorNotifier;

    @Mock
    private DownstreamTupleSender downstreamTupleSender;

    private PipelineReplica pipeline;

    private PipelineReplicaRunner runner;

    private Thread thread;

    private final int inputOutputPortCount = 1;

    private final PipelineReplicaId id = new PipelineReplicaId( new PipelineId( 0, 0 ), 0 );

    @Before
    public void init () throws Exception
    {
        when( operator.getOperatorDef() ).thenReturn( operatorDef );
        when( operatorDef.id() ).thenReturn( "op1" );
        when( operatorDef.inputPortCount() ).thenReturn( inputOutputPortCount );
        when( operatorDef.outputPortCount() ).thenReturn( inputOutputPortCount );
        final JokerConfig config = new JokerConfig();
        pipeline = new PipelineReplica( config, id, new OperatorReplica[] { operator }, mock( TupleQueueContext.class ) );
        runner = new PipelineReplicaRunner( config, pipeline, supervisor, supervisorNotifier, downstreamTupleSender );

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
    public void shouldPauseWhileRunning () throws ExecutionException, InterruptedException
    {
        thread.start();

        runner.pause().get();

        assertTrueEventually( () ->
                              {
                                  assertEquals( PAUSED, runner.getStatus() );
                              } );
    }

    @Test
    public void shouldPauseWhenAlreadyPaused () throws ExecutionException, InterruptedException
    {
        thread.start();

        runner.pause().get();

        assertTrueEventually( () ->
                              {
                                  assertEquals( PAUSED, runner.getStatus() );
                              } );

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

        when( supervisorNotifier.isPipelineCompleted() ).thenReturn( true );

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

        when( supervisorNotifier.isPipelineCompleted() ).thenReturn( true );

        assertTrueEventually( () -> assertEquals( runner.getStatus(), COMPLETED ) );

        runner.resume().get();
    }

    @Test
    public void shouldCompleteWhileRunning () throws ExecutionException, InterruptedException
    {
        final CountDownLatch invocationStartLatch = new CountDownLatch( 1 );
        final CountDownLatch invocationDoneLatch = new CountDownLatch( 1 );
        final TuplesImpl output = new TuplesImpl( 1 );
        output.add( new Tuple() );

        when( operator.invoke( anyObject(), anyObject() ) ).thenAnswer( invocation ->
                                                                        {
                                                                            invocationStartLatch.countDown();
                                                                            invocationDoneLatch.await( 2, TimeUnit.MINUTES );
                                                                            return output;
                                                                        } );

        thread.start();

        reset( supervisor );

        when( supervisorNotifier.isPipelineCompleted() ).thenReturn( true );

        invocationStartLatch.await( 2, TimeUnit.MINUTES );
        invocationDoneLatch.countDown();

        assertTrueEventually( () -> assertEquals( runner.getStatus(), COMPLETED ) );
        verify( downstreamTupleSender ).send( output );
    }

    @Test
    public void shouldUpdateUpstreamContextWhenPaused () throws ExecutionException, InterruptedException
    {
        thread.start();

        runner.pause().get();

        assertTrueEventually( () -> assertEquals( runner.getStatus(), PAUSED ) );

        final UpstreamContext upstreamContext = new UpstreamContext( 1, new UpstreamConnectionStatus[] { CLOSED } );
        when( supervisor.getUpstreamContext( pipeline.id() ) ).thenReturn( upstreamContext );

        final CompletableFuture<Boolean> future = runner.updatePipelineUpstreamContext();
        assertTrueEventually( () -> verify( supervisor, times( 2 ) ).getUpstreamContext( id ) );
        when( supervisorNotifier.isPipelineCompleted() ).thenReturn( true );
        assertTrue( future.get() );

        assertThat( runner.getPipelineUpstreamContext(), equalTo( upstreamContext ) );
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
        output1.add( new Tuple( "k1", "v1" ) );
        final TuplesImpl output2 = new TuplesImpl( inputOutputPortCount );
        output2.add( new Tuple( "k2", "v2" ) );

        when( operator.invoke( anyObject(), anyObject() ) ).thenReturn( output1, output2 );

        thread.start();

        sleepUninterruptibly( 1, SECONDS );
        when( supervisorNotifier.isPipelineCompleted() ).thenReturn( true );

        assertTrueEventually( () -> assertEquals( runner.getStatus(), COMPLETED ), 10 );
        verify( downstreamTupleSender, atLeastOnce() ).send( output1 );
        verify( downstreamTupleSender, atLeastOnce() ).send( output2 );
    }

    @Test
    public void shouldCompleteRunningWhenPipelineFailsDuringInvocations ()
    {
        final RuntimeException failure = new RuntimeException( "expected" );
        when( operator.invoke( anyObject(), anyObject() ) ).thenThrow( failure );

        thread.start();

        assertTrueEventually( () -> assertEquals( runner.getStatus(), COMPLETED ), 10 );
        verify( supervisor ).notifyPipelineReplicaFailed( id, failure );
    }

}
