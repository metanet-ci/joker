package cs.bilkent.joker.engine.pipeline;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import cs.bilkent.joker.engine.config.JokerConfig;
import static cs.bilkent.joker.engine.pipeline.OperatorReplicaStatus.COMPLETED;
import static cs.bilkent.joker.engine.pipeline.OperatorReplicaStatus.INITIALIZATION_FAILED;
import static cs.bilkent.joker.engine.pipeline.OperatorReplicaStatus.RUNNING;
import cs.bilkent.joker.engine.region.RegionConfig;
import cs.bilkent.joker.engine.supervisor.Supervisor;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.scheduling.SchedulingStrategy;
import cs.bilkent.joker.testutils.AbstractJokerTest;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PipelineTest extends AbstractJokerTest
{

    private final PipelineId pipelineId = new PipelineId( 0, 0 );

    @Test
    public void shouldInitializePipelineReplicas ()
    {
        final PipelineReplica pipelineReplica0 = mock( PipelineReplica.class ), pipelineReplica1 = mock( PipelineReplica.class );
        final PipelineReplica[] pipelineReplicas = new PipelineReplica[] { pipelineReplica0, pipelineReplica1 };
        final SchedulingStrategy initialSchedulingStrategy = mock( SchedulingStrategy.class );

        final UpstreamContext upstreamContext = mock( UpstreamContext.class );
        when( pipelineReplica0.init( upstreamContext ) ).thenReturn( new SchedulingStrategy[] { initialSchedulingStrategy } );
        when( pipelineReplica1.init( upstreamContext ) ).thenReturn( new SchedulingStrategy[] { initialSchedulingStrategy } );
        final RegionConfig regionConfig = mock( RegionConfig.class );
        when( regionConfig.getReplicaCount() ).thenReturn( 2 );
        when( regionConfig.getOperatorCountByPipelineId( pipelineId.pipelineId ) ).thenReturn( 1 );
        final Pipeline pipeline = new Pipeline( pipelineId, regionConfig, pipelineReplicas );
        pipeline.setUpstreamContext( upstreamContext );
        pipeline.init();

        assertThat( pipeline.getPipelineStatus(), equalTo( RUNNING ) );
        assertThat( pipeline.getInitialSchedulingStrategy(), equalTo( initialSchedulingStrategy ) );
    }

    @Test
    public void shouldFailInitializationWhenPipelineReplicasReturnMismatchingSchedulingStrategies ()
    {
        final PipelineReplica pipelineReplica0 = mock( PipelineReplica.class ), pipelineReplica1 = mock( PipelineReplica.class );
        final PipelineReplica[] pipelineReplicas = new PipelineReplica[] { pipelineReplica0, pipelineReplica1 };
        final SchedulingStrategy initialSchedulingStrategy = mock( SchedulingStrategy.class );
        final SchedulingStrategy initialSchedulingStrategy2 = mock( SchedulingStrategy.class );

        final UpstreamContext upstreamContext = mock( UpstreamContext.class );
        when( pipelineReplica0.init( upstreamContext ) ).thenReturn( new SchedulingStrategy[] { initialSchedulingStrategy } );
        when( pipelineReplica1.init( upstreamContext ) ).thenReturn( new SchedulingStrategy[] { initialSchedulingStrategy2 } );
        final RegionConfig regionConfig = mock( RegionConfig.class );
        when( regionConfig.getReplicaCount() ).thenReturn( 2 );
        when( regionConfig.getOperatorCountByPipelineId( pipelineId.pipelineId ) ).thenReturn( 1 );

        final Pipeline pipeline = new Pipeline( pipelineId, regionConfig, pipelineReplicas );
        pipeline.setUpstreamContext( upstreamContext );

        try
        {
            pipeline.init();
            fail();
        }
        catch ( IllegalStateException e )
        {
            assertThat( pipeline.getPipelineStatus(), equalTo( INITIALIZATION_FAILED ) );
        }
    }

    @Test
    public void shouldStartPipelineReplicaRunnersOnceInitialized ()
    {
        final PipelineReplica pipelineReplica0 = mock( PipelineReplica.class ), pipelineReplica1 = mock( PipelineReplica.class );
        final PipelineReplicaId pipelineReplicaId0 = new PipelineReplicaId( pipelineId, 0 );
        when( pipelineReplica0.id() ).thenReturn( pipelineReplicaId0 );
        final PipelineReplicaId pipelineReplicaId1 = new PipelineReplicaId( pipelineId, 1 );
        when( pipelineReplica1.id() ).thenReturn( pipelineReplicaId1 );
        final PipelineReplica[] pipelineReplicas = new PipelineReplica[] { pipelineReplica0, pipelineReplica1 };
        final SchedulingStrategy initialSchedulingStrategy = mock( SchedulingStrategy.class );

        final UpstreamContext upstreamContext = mock( UpstreamContext.class );

        when( pipelineReplica0.init( upstreamContext ) ).thenReturn( new SchedulingStrategy[] { initialSchedulingStrategy } );
        when( pipelineReplica1.init( upstreamContext ) ).thenReturn( new SchedulingStrategy[] { initialSchedulingStrategy } );
        final RegionConfig regionConfig = mock( RegionConfig.class );
        when( regionConfig.getReplicaCount() ).thenReturn( 2 );
        when( regionConfig.getOperatorCountByPipelineId( pipelineId.pipelineId ) ).thenReturn( 1 );
        final Pipeline pipeline = new Pipeline( pipelineId, regionConfig, pipelineReplicas );
        pipeline.setUpstreamContext( upstreamContext );
        pipeline.init();

        final Supervisor supervisor = mock( Supervisor.class );
        when( supervisor.getUpstreamContext( pipelineReplicaId0 ) ).thenReturn( upstreamContext );
        when( supervisor.getUpstreamContext( pipelineReplicaId1 ) ).thenReturn( upstreamContext );

        pipeline.startPipelineReplicaRunners( new JokerConfig(), supervisor, new ThreadGroup( "test" ) );

        pipeline.stopPipelineReplicaRunners( false, TimeUnit.SECONDS.toMillis( 30 ) );
    }

    @Test
    public void shouldNotStartPipelineReplicaRunnersMultipleTimes ()
    {
        final PipelineReplica pipelineReplica0 = mock( PipelineReplica.class ), pipelineReplica1 = mock( PipelineReplica.class );
        final PipelineReplicaId pipelineReplicaId0 = new PipelineReplicaId( pipelineId, 0 );
        when( pipelineReplica0.id() ).thenReturn( pipelineReplicaId0 );
        final PipelineReplicaId pipelineReplicaId1 = new PipelineReplicaId( pipelineId, 1 );
        when( pipelineReplica1.id() ).thenReturn( pipelineReplicaId1 );
        final PipelineReplica[] pipelineReplicas = new PipelineReplica[] { pipelineReplica0, pipelineReplica1 };
        final SchedulingStrategy initialSchedulingStrategy = mock( SchedulingStrategy.class );

        final UpstreamContext upstreamContext = mock( UpstreamContext.class );

        when( pipelineReplica0.init( upstreamContext ) ).thenReturn( new SchedulingStrategy[] { initialSchedulingStrategy } );
        when( pipelineReplica1.init( upstreamContext ) ).thenReturn( new SchedulingStrategy[] { initialSchedulingStrategy } );
        final RegionConfig regionConfig = mock( RegionConfig.class );
        when( regionConfig.getReplicaCount() ).thenReturn( 2 );
        when( regionConfig.getOperatorCountByPipelineId( pipelineId.pipelineId ) ).thenReturn( 1 );
        final Pipeline pipeline = new Pipeline( pipelineId, regionConfig, pipelineReplicas );
        pipeline.setUpstreamContext( upstreamContext );
        pipeline.init();

        final Supervisor supervisor = mock( Supervisor.class );
        when( supervisor.getUpstreamContext( pipelineReplicaId0 ) ).thenReturn( upstreamContext );
        when( supervisor.getUpstreamContext( pipelineReplicaId1 ) ).thenReturn( upstreamContext );

        pipeline.startPipelineReplicaRunners( new JokerConfig(), supervisor, new ThreadGroup( "test" ) );
        try
        {
            pipeline.startPipelineReplicaRunners( new JokerConfig(), supervisor, new ThreadGroup( "test" ) );
            fail();
        }
        catch ( IllegalStateException expected )
        {

        }

        pipeline.stopPipelineReplicaRunners( false, TimeUnit.SECONDS.toMillis( 30 ) );
    }

    @Test( expected = IllegalStateException.class )
    public void shouldNotStopPipelineReplicaRunnersIfNotStarted ()
    {
        final PipelineReplica pipelineReplica0 = mock( PipelineReplica.class ), pipelineReplica1 = mock( PipelineReplica.class );
        final PipelineReplicaId pipelineReplicaId0 = new PipelineReplicaId( pipelineId, 0 );
        when( pipelineReplica0.id() ).thenReturn( pipelineReplicaId0 );
        final PipelineReplicaId pipelineReplicaId1 = new PipelineReplicaId( pipelineId, 1 );
        when( pipelineReplica1.id() ).thenReturn( pipelineReplicaId1 );
        final PipelineReplica[] pipelineReplicas = new PipelineReplica[] { pipelineReplica0, pipelineReplica1 };
        final SchedulingStrategy initialSchedulingStrategy = mock( SchedulingStrategy.class );

        final UpstreamContext upstreamContext = mock( UpstreamContext.class );

        when( pipelineReplica0.init( upstreamContext ) ).thenReturn( new SchedulingStrategy[] { initialSchedulingStrategy } );
        when( pipelineReplica1.init( upstreamContext ) ).thenReturn( new SchedulingStrategy[] { initialSchedulingStrategy } );
        final RegionConfig regionConfig = mock( RegionConfig.class );
        when( regionConfig.getReplicaCount() ).thenReturn( 2 );
        when( regionConfig.getOperatorCountByPipelineId( pipelineId.pipelineId ) ).thenReturn( 1 );
        final Pipeline pipeline = new Pipeline( pipelineId, regionConfig, pipelineReplicas );
        pipeline.setUpstreamContext( upstreamContext );
        pipeline.init();

        pipeline.stopPipelineReplicaRunners( false, TimeUnit.SECONDS.toMillis( 30 ) );
    }

    @Test
    public void shouldNotShutdownOperatorsIfRunnersNotStopped ()
    {
        final PipelineReplica pipelineReplica0 = mock( PipelineReplica.class ), pipelineReplica1 = mock( PipelineReplica.class );
        final PipelineReplicaId pipelineReplicaId0 = new PipelineReplicaId( pipelineId, 0 );
        when( pipelineReplica0.id() ).thenReturn( pipelineReplicaId0 );
        final PipelineReplicaId pipelineReplicaId1 = new PipelineReplicaId( pipelineId, 1 );
        when( pipelineReplica1.id() ).thenReturn( pipelineReplicaId1 );
        final PipelineReplica[] pipelineReplicas = new PipelineReplica[] { pipelineReplica0, pipelineReplica1 };
        final SchedulingStrategy initialSchedulingStrategy = mock( SchedulingStrategy.class );

        final UpstreamContext upstreamContext = mock( UpstreamContext.class );

        when( pipelineReplica0.init( upstreamContext ) ).thenReturn( new SchedulingStrategy[] { initialSchedulingStrategy } );
        when( pipelineReplica1.init( upstreamContext ) ).thenReturn( new SchedulingStrategy[] { initialSchedulingStrategy } );
        final RegionConfig regionConfig = mock( RegionConfig.class );
        when( regionConfig.getReplicaCount() ).thenReturn( 2 );
        when( regionConfig.getOperatorCountByPipelineId( pipelineId.pipelineId ) ).thenReturn( 1 );
        final Pipeline pipeline = new Pipeline( pipelineId, regionConfig, pipelineReplicas );
        pipeline.setUpstreamContext( upstreamContext );
        pipeline.init();

        final Supervisor supervisor = mock( Supervisor.class );
        when( supervisor.getUpstreamContext( pipelineReplicaId0 ) ).thenReturn( upstreamContext );
        when( supervisor.getUpstreamContext( pipelineReplicaId1 ) ).thenReturn( upstreamContext );

        pipeline.startPipelineReplicaRunners( new JokerConfig(), supervisor, new ThreadGroup( "test" ) );

        try
        {
            pipeline.shutdown();
            fail();
        }
        catch ( Exception e )
        {
            pipeline.stopPipelineReplicaRunners( false, TimeUnit.SECONDS.toMillis( 30 ) );
        }
    }

    @Test
    public void shouldCompleteRunnerWhenUpstreamContextIsUpdated ()
    {
        final PipelineReplica pipelineReplica0 = mock( PipelineReplica.class ), pipelineReplica1 = mock( PipelineReplica.class );
        final PipelineReplicaId pipelineReplicaId0 = new PipelineReplicaId( pipelineId, 0 );
        when( pipelineReplica0.id() ).thenReturn( pipelineReplicaId0 );
        final PipelineReplicaId pipelineReplicaId1 = new PipelineReplicaId( pipelineId, 1 );
        when( pipelineReplica1.id() ).thenReturn( pipelineReplicaId1 );
        final PipelineReplica[] pipelineReplicas = new PipelineReplica[] { pipelineReplica0, pipelineReplica1 };
        final SchedulingStrategy initialSchedulingStrategy = mock( SchedulingStrategy.class );

        final UpstreamContext upstreamContext = mock( UpstreamContext.class );

        when( pipelineReplica0.init( upstreamContext ) ).thenReturn( new SchedulingStrategy[] { initialSchedulingStrategy } );
        when( pipelineReplica1.init( upstreamContext ) ).thenReturn( new SchedulingStrategy[] { initialSchedulingStrategy } );
        final RegionConfig regionConfig = mock( RegionConfig.class );
        when( regionConfig.getReplicaCount() ).thenReturn( 2 );
        when( regionConfig.getOperatorCountByPipelineId( pipelineId.pipelineId ) ).thenReturn( 1 );
        final Pipeline pipeline = new Pipeline( pipelineId, regionConfig, pipelineReplicas );
        pipeline.setUpstreamContext( upstreamContext );
        pipeline.init();

        final Supervisor supervisor = mock( Supervisor.class );
        when( supervisor.getUpstreamContext( pipelineReplicaId0 ) ).thenReturn( upstreamContext );
        when( supervisor.getUpstreamContext( pipelineReplicaId1 ) ).thenReturn( upstreamContext );

        final AtomicBoolean completed = new AtomicBoolean( false );
        when( pipelineReplica0.isCompleted() ).thenAnswer( new Answer<Boolean>()
        {
            @Override
            public Boolean answer ( final InvocationOnMock invocation ) throws Throwable
            {
                return completed.get();
            }
        } );

        when( pipelineReplica1.isCompleted() ).thenAnswer( new Answer<Boolean>()
        {
            @Override
            public Boolean answer ( final InvocationOnMock invocation ) throws Throwable
            {
                return completed.get();
            }
        } );

        pipeline.startPipelineReplicaRunners( new JokerConfig(), supervisor, new ThreadGroup( "test" ) );

        final UpstreamContext newUpstreamContext = mock( UpstreamContext.class );
        when( newUpstreamContext.getVersion() ).thenReturn( 1 );
        reset( supervisor );
        when( supervisor.getUpstreamContext( pipelineReplicaId0 ) ).thenReturn( newUpstreamContext );
        when( supervisor.getUpstreamContext( pipelineReplicaId1 ) ).thenReturn( newUpstreamContext );

        final OperatorDef operatorDef = mock( OperatorDef.class );
        when( regionConfig.getOperatorDefByPipelineId( pipelineId.pipelineId, 0 ) ).thenReturn( operatorDef );

        pipeline.handleUpstreamContextUpdated( newUpstreamContext );

        assertTrueEventually( () ->
                              {
                                  verify( pipelineReplica0 ).setPipelineUpstreamContext( newUpstreamContext );
                                  verify( pipelineReplica1 ).setPipelineUpstreamContext( newUpstreamContext );
                              } );

        verify( newUpstreamContext ).isInvokable( operatorDef, initialSchedulingStrategy );

        completed.set( true );

        assertTrueEventually( () ->
                              {
                                  verify( supervisor ).notifyPipelineReplicaCompleted( pipelineReplicaId0 );
                                  verify( supervisor ).notifyPipelineReplicaCompleted( pipelineReplicaId1 );
                                  pipeline.handlePipelineReplicaCompleted( 0 );
                                  pipeline.handlePipelineReplicaCompleted( 1 );
                              } );

        assertThat( pipeline.getPipelineStatus(), equalTo( COMPLETED ) );

        pipeline.stopPipelineReplicaRunners( false, TimeUnit.SECONDS.toMillis( 30 ) );
    }

}
