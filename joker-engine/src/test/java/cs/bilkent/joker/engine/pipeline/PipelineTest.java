package cs.bilkent.joker.engine.pipeline;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;
import org.mockito.stubbing.Answer;

import cs.bilkent.joker.engine.config.JokerConfig;
import cs.bilkent.joker.engine.flow.PipelineId;
import cs.bilkent.joker.engine.flow.RegionDef;
import cs.bilkent.joker.engine.flow.RegionExecPlan;
import static cs.bilkent.joker.engine.pipeline.OperatorReplicaStatus.COMPLETED;
import static cs.bilkent.joker.engine.pipeline.OperatorReplicaStatus.RUNNING;
import static cs.bilkent.joker.engine.pipeline.UpstreamCtx.INITIAL_VERSION;
import cs.bilkent.joker.engine.region.Region;
import cs.bilkent.joker.engine.supervisor.Supervisor;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.scheduling.SchedulingStrategy;
import cs.bilkent.joker.test.AbstractJokerTest;
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
        final SchedulingStrategy schedulingStrategy = mock( SchedulingStrategy.class );
        final SchedulingStrategy[] schedulingStrategies = new SchedulingStrategy[] { schedulingStrategy };
        final SchedulingStrategy[][] fusedSchedulingStrategies = new SchedulingStrategy[][] { { schedulingStrategy } };
        final UpstreamCtx upstreamCtx = mock( UpstreamCtx.class );
        final UpstreamCtx[] upstreamCtxes = new UpstreamCtx[] { upstreamCtx };
        final UpstreamCtx[][] fusedUpstreamCtxes = new UpstreamCtx[][] { { upstreamCtx } };

        when( pipelineReplica0.getStatus() ).thenReturn( OperatorReplicaStatus.INITIAL );
        when( pipelineReplica1.getStatus() ).thenReturn( OperatorReplicaStatus.INITIAL );

        final RegionExecPlan regionExecPlan = mock( RegionExecPlan.class );
        when( regionExecPlan.getReplicaCount() ).thenReturn( 2 );
        when( regionExecPlan.getOperatorCountByPipelineStartIndex( pipelineId.getPipelineStartIndex() ) ).thenReturn( 1 );

        final Region region = mock( Region.class );
        when( region.getPipelineReplicas( pipelineId ) ).thenReturn( new PipelineReplica[] { pipelineReplica0, pipelineReplica1 } );
        when( region.getSchedulingStrategies( pipelineId ) ).thenReturn( schedulingStrategies );
        when( region.getFusedSchedulingStrategies( pipelineId ) ).thenReturn( fusedSchedulingStrategies );
        when( region.getUpstreamCtxes( pipelineId ) ).thenReturn( upstreamCtxes );
        when( region.getFusedUpstreamCtxes( pipelineId ) ).thenReturn( fusedUpstreamCtxes );
        when( region.getExecPlan() ).thenReturn( regionExecPlan );

        final Pipeline pipeline = new Pipeline( pipelineId, region );
        pipeline.init();

        assertThat( pipeline.getPipelineStatus(), equalTo( RUNNING ) );
        assertThat( pipeline.getUpstreamCtx(), equalTo( upstreamCtxes[ 0 ] ) );

        verify( pipelineReplica0 ).init( fusedSchedulingStrategies, fusedUpstreamCtxes );
        verify( pipelineReplica1 ).init( fusedSchedulingStrategies, fusedUpstreamCtxes );
    }

    @Test
    public void shouldStartPipelineReplicaRunnersOnceInitialized ()
    {
        final PipelineReplica pipelineReplica0 = mock( PipelineReplica.class ), pipelineReplica1 = mock( PipelineReplica.class );
        final PipelineReplicaId pipelineReplicaId0 = new PipelineReplicaId( pipelineId, 0 );
        when( pipelineReplica0.id() ).thenReturn( pipelineReplicaId0 );
        final PipelineReplicaId pipelineReplicaId1 = new PipelineReplicaId( pipelineId, 1 );
        when( pipelineReplica1.id() ).thenReturn( pipelineReplicaId1 );
        final SchedulingStrategy schedulingStrategy = mock( SchedulingStrategy.class );
        final SchedulingStrategy[] schedulingStrategies = new SchedulingStrategy[] { schedulingStrategy };
        final SchedulingStrategy[][] fusedSchedulingStrategies = new SchedulingStrategy[][] { { schedulingStrategy } };
        final UpstreamCtx upstreamCtx = mock( UpstreamCtx.class );
        final UpstreamCtx[] upstreamCtxes = new UpstreamCtx[] { upstreamCtx };
        final UpstreamCtx[][] fusedUpstreamCtxes = new UpstreamCtx[][] { { upstreamCtx } };

        when( pipelineReplica0.getStatus() ).thenReturn( OperatorReplicaStatus.INITIAL );
        when( pipelineReplica1.getStatus() ).thenReturn( OperatorReplicaStatus.INITIAL );

        final RegionExecPlan regionExecPlan = mock( RegionExecPlan.class );
        when( regionExecPlan.getReplicaCount() ).thenReturn( 2 );
        when( regionExecPlan.getOperatorCountByPipelineStartIndex( pipelineId.getPipelineStartIndex() ) ).thenReturn( 1 );

        final RegionDef regionDef = mock( RegionDef.class );
        when( regionExecPlan.getRegionDef() ).thenReturn( regionDef );

        final Region region = mock( Region.class );
        when( region.getPipelineReplicas( pipelineId ) ).thenReturn( new PipelineReplica[] { pipelineReplica0, pipelineReplica1 } );
        when( region.getSchedulingStrategies( pipelineId ) ).thenReturn( schedulingStrategies );
        when( region.getFusedSchedulingStrategies( pipelineId ) ).thenReturn( fusedSchedulingStrategies );
        when( region.getUpstreamCtxes( pipelineId ) ).thenReturn( upstreamCtxes );
        when( region.getFusedUpstreamCtxes( pipelineId ) ).thenReturn( fusedUpstreamCtxes );
        when( region.getExecPlan() ).thenReturn( regionExecPlan );

        final Pipeline pipeline = new Pipeline( pipelineId, region );
        pipeline.init();

        final Supervisor supervisor = mock( Supervisor.class );
        when( supervisor.getUpstreamCtx( pipelineReplicaId0 ) ).thenReturn( upstreamCtxes[ 0 ] );
        when( supervisor.getUpstreamCtx( pipelineReplicaId1 ) ).thenReturn( upstreamCtxes[ 0 ] );
        pipeline.setDownstreamCollectors( new DownstreamCollector[] { mock( DownstreamCollector.class ),
                                                                      mock( DownstreamCollector.class ) } );
        when( supervisor.getDownstreamCollector( pipelineReplicaId0 ) ).thenReturn( mock( DownstreamCollector.class ) );
        when( supervisor.getDownstreamCollector( pipelineReplicaId1 ) ).thenReturn( mock( DownstreamCollector.class ) );

        pipeline.startPipelineReplicaRunners( new JokerConfig(), supervisor, new ThreadGroup( "test" ) );

        pipeline.stopPipelineReplicaRunners( TimeUnit.SECONDS.toMillis( 30 ) );
    }

    @Test
    public void shouldNotStartPipelineReplicaRunnersMultipleTimes ()
    {
        final PipelineReplica pipelineReplica0 = mock( PipelineReplica.class ), pipelineReplica1 = mock( PipelineReplica.class );
        final PipelineReplicaId pipelineReplicaId0 = new PipelineReplicaId( pipelineId, 0 );
        when( pipelineReplica0.id() ).thenReturn( pipelineReplicaId0 );
        final PipelineReplicaId pipelineReplicaId1 = new PipelineReplicaId( pipelineId, 1 );
        when( pipelineReplica1.id() ).thenReturn( pipelineReplicaId1 );
        final SchedulingStrategy schedulingStrategy = mock( SchedulingStrategy.class );
        final SchedulingStrategy[] schedulingStrategies = new SchedulingStrategy[] { schedulingStrategy };
        final SchedulingStrategy[][] fusedSchedulingStrategies = new SchedulingStrategy[][] { { schedulingStrategy } };
        final UpstreamCtx upstreamCtx = mock( UpstreamCtx.class );
        final UpstreamCtx[] upstreamCtxes = new UpstreamCtx[] { upstreamCtx };
        final UpstreamCtx[][] fusedUpstreamCtxes = new UpstreamCtx[][] { { upstreamCtx } };

        when( pipelineReplica0.getStatus() ).thenReturn( OperatorReplicaStatus.INITIAL );
        when( pipelineReplica1.getStatus() ).thenReturn( OperatorReplicaStatus.INITIAL );

        final RegionExecPlan regionExecPlan = mock( RegionExecPlan.class );
        when( regionExecPlan.getReplicaCount() ).thenReturn( 2 );
        when( regionExecPlan.getOperatorCountByPipelineStartIndex( pipelineId.getPipelineStartIndex() ) ).thenReturn( 1 );

        final RegionDef regionDef = mock( RegionDef.class );
        when( regionExecPlan.getRegionDef() ).thenReturn( regionDef );

        final Region region = mock( Region.class );
        when( region.getPipelineReplicas( pipelineId ) ).thenReturn( new PipelineReplica[] { pipelineReplica0, pipelineReplica1 } );
        when( region.getSchedulingStrategies( pipelineId ) ).thenReturn( schedulingStrategies );
        when( region.getFusedSchedulingStrategies( pipelineId ) ).thenReturn( fusedSchedulingStrategies );
        when( region.getUpstreamCtxes( pipelineId ) ).thenReturn( upstreamCtxes );
        when( region.getFusedUpstreamCtxes( pipelineId ) ).thenReturn( fusedUpstreamCtxes );
        when( region.getExecPlan() ).thenReturn( regionExecPlan );

        final Pipeline pipeline = new Pipeline( pipelineId, region );
        pipeline.init();

        final Supervisor supervisor = mock( Supervisor.class );
        when( supervisor.getUpstreamCtx( pipelineReplicaId0 ) ).thenReturn( upstreamCtxes[ 0 ] );
        when( supervisor.getUpstreamCtx( pipelineReplicaId1 ) ).thenReturn( upstreamCtxes[ 0 ] );
        when( supervisor.getDownstreamCollector( pipelineReplicaId0 ) ).thenReturn( mock( DownstreamCollector.class ) );
        when( supervisor.getDownstreamCollector( pipelineReplicaId1 ) ).thenReturn( mock( DownstreamCollector.class ) );
        pipeline.setDownstreamCollectors( new DownstreamCollector[] { mock( DownstreamCollector.class ),
                                                                      mock( DownstreamCollector.class ) } );

        pipeline.startPipelineReplicaRunners( new JokerConfig(), supervisor, new ThreadGroup( "test" ) );
        try
        {
            pipeline.startPipelineReplicaRunners( new JokerConfig(), supervisor, new ThreadGroup( "test" ) );
            fail();
        }
        catch ( IllegalStateException ignored )
        {
        }

        pipeline.stopPipelineReplicaRunners( TimeUnit.SECONDS.toMillis( 30 ) );
    }

    @Test( expected = IllegalStateException.class )
    public void shouldNotStopPipelineReplicaRunnersIfNotStarted ()
    {
        final PipelineReplica pipelineReplica0 = mock( PipelineReplica.class ), pipelineReplica1 = mock( PipelineReplica.class );
        final PipelineReplicaId pipelineReplicaId0 = new PipelineReplicaId( pipelineId, 0 );
        when( pipelineReplica0.id() ).thenReturn( pipelineReplicaId0 );
        final PipelineReplicaId pipelineReplicaId1 = new PipelineReplicaId( pipelineId, 1 );
        when( pipelineReplica1.id() ).thenReturn( pipelineReplicaId1 );
        final SchedulingStrategy schedulingStrategy = mock( SchedulingStrategy.class );
        final SchedulingStrategy[] schedulingStrategies = new SchedulingStrategy[] { schedulingStrategy };
        final SchedulingStrategy[][] fusedSchedulingStrategies = new SchedulingStrategy[][] { { schedulingStrategy } };
        final UpstreamCtx upstreamCtx = mock( UpstreamCtx.class );
        final UpstreamCtx[] upstreamCtxes = new UpstreamCtx[] { upstreamCtx };
        final UpstreamCtx[][] fusedUpstreamCtxes = new UpstreamCtx[][] { { upstreamCtx } };

        when( pipelineReplica0.getStatus() ).thenReturn( OperatorReplicaStatus.INITIAL );
        when( pipelineReplica1.getStatus() ).thenReturn( OperatorReplicaStatus.INITIAL );

        final RegionExecPlan regionExecPlan = mock( RegionExecPlan.class );
        when( regionExecPlan.getReplicaCount() ).thenReturn( 2 );
        when( regionExecPlan.getOperatorCountByPipelineStartIndex( pipelineId.getPipelineStartIndex() ) ).thenReturn( 1 );

        final Region region = mock( Region.class );
        when( region.getPipelineReplicas( pipelineId ) ).thenReturn( new PipelineReplica[] { pipelineReplica0, pipelineReplica1 } );
        when( region.getSchedulingStrategies( pipelineId ) ).thenReturn( schedulingStrategies );
        when( region.getFusedSchedulingStrategies( pipelineId ) ).thenReturn( fusedSchedulingStrategies );
        when( region.getUpstreamCtxes( pipelineId ) ).thenReturn( upstreamCtxes );
        when( region.getFusedUpstreamCtxes( pipelineId ) ).thenReturn( fusedUpstreamCtxes );
        when( region.getExecPlan() ).thenReturn( regionExecPlan );

        final Pipeline pipeline = new Pipeline( pipelineId, region );
        pipeline.init();

        pipeline.stopPipelineReplicaRunners( TimeUnit.SECONDS.toMillis( 30 ) );
    }

    @Test
    public void shouldNotShutdownOperatorsIfRunnersNotStopped ()
    {
        final PipelineReplica pipelineReplica0 = mock( PipelineReplica.class ), pipelineReplica1 = mock( PipelineReplica.class );
        final PipelineReplicaId pipelineReplicaId0 = new PipelineReplicaId( pipelineId, 0 );
        when( pipelineReplica0.id() ).thenReturn( pipelineReplicaId0 );
        final PipelineReplicaId pipelineReplicaId1 = new PipelineReplicaId( pipelineId, 1 );
        when( pipelineReplica1.id() ).thenReturn( pipelineReplicaId1 );
        final SchedulingStrategy schedulingStrategy = mock( SchedulingStrategy.class );
        final SchedulingStrategy[] schedulingStrategies = new SchedulingStrategy[] { schedulingStrategy };
        final SchedulingStrategy[][] fusedSchedulingStrategies = new SchedulingStrategy[][] { { schedulingStrategy } };
        final UpstreamCtx upstreamCtx = mock( UpstreamCtx.class );
        final UpstreamCtx[] upstreamCtxes = new UpstreamCtx[] { upstreamCtx };
        final UpstreamCtx[][] fusedUpstreamCtxes = new UpstreamCtx[][] { { upstreamCtx } };

        when( pipelineReplica0.getStatus() ).thenReturn( OperatorReplicaStatus.INITIAL );
        when( pipelineReplica1.getStatus() ).thenReturn( OperatorReplicaStatus.INITIAL );

        final RegionExecPlan regionExecPlan = mock( RegionExecPlan.class );
        when( regionExecPlan.getReplicaCount() ).thenReturn( 2 );
        when( regionExecPlan.getOperatorCountByPipelineStartIndex( pipelineId.getPipelineStartIndex() ) ).thenReturn( 1 );

        final RegionDef regionDef = mock( RegionDef.class );
        when( regionExecPlan.getRegionDef() ).thenReturn( regionDef );

        final Region region = mock( Region.class );
        when( region.getPipelineReplicas( pipelineId ) ).thenReturn( new PipelineReplica[] { pipelineReplica0, pipelineReplica1 } );
        when( region.getSchedulingStrategies( pipelineId ) ).thenReturn( schedulingStrategies );
        when( region.getFusedSchedulingStrategies( pipelineId ) ).thenReturn( fusedSchedulingStrategies );
        when( region.getUpstreamCtxes( pipelineId ) ).thenReturn( upstreamCtxes );
        when( region.getFusedUpstreamCtxes( pipelineId ) ).thenReturn( fusedUpstreamCtxes );
        when( region.getExecPlan() ).thenReturn( regionExecPlan );

        final Pipeline pipeline = new Pipeline( pipelineId, region );
        pipeline.init();

        final Supervisor supervisor = mock( Supervisor.class );
        when( supervisor.getUpstreamCtx( pipelineReplicaId0 ) ).thenReturn( upstreamCtxes[ 0 ] );
        when( supervisor.getUpstreamCtx( pipelineReplicaId1 ) ).thenReturn( upstreamCtxes[ 0 ] );
        when( supervisor.getDownstreamCollector( pipelineReplicaId0 ) ).thenReturn( mock( DownstreamCollector.class ) );
        when( supervisor.getDownstreamCollector( pipelineReplicaId1 ) ).thenReturn( mock( DownstreamCollector.class ) );
        pipeline.setDownstreamCollectors( new DownstreamCollector[] { mock( DownstreamCollector.class ),
                                                                      mock( DownstreamCollector.class ) } );

        pipeline.startPipelineReplicaRunners( new JokerConfig(), supervisor, new ThreadGroup( "test" ) );
        try
        {
            pipeline.shutdown();
            fail();
        }
        catch ( Exception e )
        {
            pipeline.stopPipelineReplicaRunners( TimeUnit.SECONDS.toMillis( 30 ) );
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
        final SchedulingStrategy schedulingStrategy = mock( SchedulingStrategy.class );
        final SchedulingStrategy[] schedulingStrategies = new SchedulingStrategy[] { schedulingStrategy };
        final SchedulingStrategy[][] fusedSchedulingStrategies = new SchedulingStrategy[][] { { schedulingStrategy } };
        final UpstreamCtx upstreamCtx = mock( UpstreamCtx.class );
        final UpstreamCtx[] upstreamCtxes = new UpstreamCtx[] { upstreamCtx };
        final UpstreamCtx[][] fusedUpstreamCtxes = new UpstreamCtx[][] { { upstreamCtx } };

        when( pipelineReplica0.getStatus() ).thenReturn( OperatorReplicaStatus.INITIAL );
        when( pipelineReplica1.getStatus() ).thenReturn( OperatorReplicaStatus.INITIAL );

        final RegionExecPlan regionExecPlan = mock( RegionExecPlan.class );
        when( regionExecPlan.getReplicaCount() ).thenReturn( 2 );
        when( regionExecPlan.getOperatorCountByPipelineStartIndex( pipelineId.getPipelineStartIndex() ) ).thenReturn( 1 );

        final RegionDef regionDef = mock( RegionDef.class );
        when( regionExecPlan.getRegionDef() ).thenReturn( regionDef );

        final Region region = mock( Region.class );
        when( region.getPipelineReplicas( pipelineId ) ).thenReturn( new PipelineReplica[] { pipelineReplica0, pipelineReplica1 } );
        when( region.getSchedulingStrategies( pipelineId ) ).thenReturn( schedulingStrategies );
        when( region.getFusedSchedulingStrategies( pipelineId ) ).thenReturn( fusedSchedulingStrategies );
        when( region.getUpstreamCtxes( pipelineId ) ).thenReturn( upstreamCtxes );
        when( region.getFusedUpstreamCtxes( pipelineId ) ).thenReturn( fusedUpstreamCtxes );
        when( region.getExecPlan() ).thenReturn( regionExecPlan );

        final Pipeline pipeline = new Pipeline( pipelineId, region );
        pipeline.setDownstreamCollectors( new DownstreamCollector[] { mock( DownstreamCollector.class ),
                                                                      mock( DownstreamCollector.class ) } );
        pipeline.init();

        final Supervisor supervisor = mock( Supervisor.class );
        when( supervisor.getUpstreamCtx( pipelineReplicaId0 ) ).thenReturn( upstreamCtxes[ 0 ] );
        when( supervisor.getUpstreamCtx( pipelineReplicaId1 ) ).thenReturn( upstreamCtxes[ 0 ] );
        when( supervisor.getDownstreamCollector( pipelineReplicaId0 ) ).thenReturn( mock( DownstreamCollector.class ) );
        when( supervisor.getDownstreamCollector( pipelineReplicaId1 ) ).thenReturn( mock( DownstreamCollector.class ) );

        final AtomicBoolean completed = new AtomicBoolean( false );
        when( pipelineReplica0.isCompleted() ).thenAnswer( (Answer<Boolean>) invocation -> completed.get() );

        when( pipelineReplica1.isCompleted() ).thenAnswer( (Answer<Boolean>) invocation -> completed.get() );

        pipeline.startPipelineReplicaRunners( new JokerConfig(), supervisor, new ThreadGroup( "test" ) );

        final UpstreamCtx newUpstreamCtx = mock( UpstreamCtx.class );
        when( newUpstreamCtx.getVersion() ).thenReturn( INITIAL_VERSION + 1 );
        reset( supervisor );
        when( supervisor.getUpstreamCtx( pipelineReplicaId0 ) ).thenReturn( newUpstreamCtx );
        when( supervisor.getUpstreamCtx( pipelineReplicaId1 ) ).thenReturn( newUpstreamCtx );
        when( supervisor.getDownstreamCollector( pipelineReplicaId0 ) ).thenReturn( mock( DownstreamCollector.class ) );
        when( supervisor.getDownstreamCollector( pipelineReplicaId1 ) ).thenReturn( mock( DownstreamCollector.class ) );

        final OperatorDef operatorDef = mock( OperatorDef.class );
        when( regionExecPlan.getOperatorDefsByPipelineStartIndex( pipelineId.getPipelineStartIndex() ) ).thenReturn( new OperatorDef[] {
                operatorDef } );

        pipeline.handleUpstreamCtxUpdated( newUpstreamCtx );

        assertTrueEventually( () -> {
            verify( pipelineReplica0 ).setUpstreamCtx( newUpstreamCtx );
            verify( pipelineReplica1 ).setUpstreamCtx( newUpstreamCtx );
        } );

        verify( newUpstreamCtx ).isInvokable( operatorDef, schedulingStrategies[ 0 ] );

        completed.set( true );

        assertTrueEventually( () -> {
            verify( supervisor ).notifyPipelineReplicaCompleted( pipelineReplicaId0 );
            verify( supervisor ).notifyPipelineReplicaCompleted( pipelineReplicaId1 );

            pipeline.handlePipelineReplicaCompleted( 0 );
            pipeline.handlePipelineReplicaCompleted( 1 );
        } );

        assertThat( pipeline.getPipelineStatus(), equalTo( COMPLETED ) );

        pipeline.stopPipelineReplicaRunners( TimeUnit.SECONDS.toMillis( 30 ) );
    }

}
