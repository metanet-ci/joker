package cs.bilkent.joker.engine.adaptation.impl.bottleneckresolver;

import org.junit.Before;
import org.junit.Test;

import cs.bilkent.joker.engine.adaptation.AdaptationAction;
import cs.bilkent.joker.engine.adaptation.impl.adaptationaction.RegionRebalanceAction;
import cs.bilkent.joker.engine.flow.PipelineId;
import cs.bilkent.joker.engine.flow.RegionExecutionPlan;
import cs.bilkent.joker.engine.metric.PipelineMetrics;
import static cs.bilkent.joker.operator.spec.OperatorType.PARTITIONED_STATEFUL;
import static cs.bilkent.joker.operator.spec.OperatorType.STATEFUL;
import static cs.bilkent.joker.operator.spec.OperatorType.STATELESS;
import cs.bilkent.joker.test.AbstractJokerTest;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RegionExtenderTest extends AbstractJokerTest
{

    private static final int MAX_REPLICA_COUNT = 3;


    private final RegionExecutionPlan regionExecutionPlan = mock( RegionExecutionPlan.class );

    private final RegionExecutionPlan newRegionExecutionPlan = mock( RegionExecutionPlan.class );

    private final PipelineId pipelineId = new PipelineId( 0, 0 );

    private final PipelineMetrics bottleneckPipelineMetrics = mock( PipelineMetrics.class );

    private RegionExtender regionExtender = new RegionExtender( MAX_REPLICA_COUNT );


    @Before
    public void init ()
    {
        when( regionExecutionPlan.withNewReplicaCount( MAX_REPLICA_COUNT ) ).thenReturn( newRegionExecutionPlan );
    }

    @Test
    public void shouldNotExtendStatefulRegion ()
    {
        when( regionExecutionPlan.getRegionType() ).thenReturn( STATEFUL );

        final AdaptationAction action = regionExtender.resolve( regionExecutionPlan, bottleneckPipelineMetrics );

        assertNull( action );
    }

    @Test
    public void shouldNotExtendStatelessRegion ()
    {
        when( regionExecutionPlan.getRegionType() ).thenReturn( STATELESS );

        final AdaptationAction action = regionExtender.resolve( regionExecutionPlan, bottleneckPipelineMetrics );

        assertNull( action );
    }

    @Test
    public void shouldNotExtendIfMaxReplicaCountIsReached ()
    {
        when( regionExecutionPlan.getRegionType() ).thenReturn( PARTITIONED_STATEFUL );
        when( regionExecutionPlan.getReplicaCount() ).thenReturn( MAX_REPLICA_COUNT );

        final AdaptationAction action = regionExtender.resolve( regionExecutionPlan, bottleneckPipelineMetrics );

        assertNull( action );
    }

    @Test
    public void shouldExtendPartitionedStatefulRegion ()
    {
        when( regionExecutionPlan.getRegionType() ).thenReturn( PARTITIONED_STATEFUL );
        when( regionExecutionPlan.getReplicaCount() ).thenReturn( MAX_REPLICA_COUNT - 1 );

        final AdaptationAction action = regionExtender.resolve( regionExecutionPlan, bottleneckPipelineMetrics );

        assertTrue( action instanceof RegionRebalanceAction );
        final RegionRebalanceAction regionRebalanceAction = (RegionRebalanceAction) action;
        assertThat( regionRebalanceAction.getCurrentRegionExecutionPlan(), equalTo( regionExecutionPlan ) );
        assertThat( regionRebalanceAction.getNewRegionExecutionPlan(), equalTo( newRegionExecutionPlan ) );
    }

}
