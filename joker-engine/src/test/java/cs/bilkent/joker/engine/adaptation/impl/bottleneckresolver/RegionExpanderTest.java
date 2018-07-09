package cs.bilkent.joker.engine.adaptation.impl.bottleneckresolver;

import org.junit.Before;
import org.junit.Test;

import cs.bilkent.joker.engine.adaptation.AdaptationAction;
import cs.bilkent.joker.engine.adaptation.impl.adaptationaction.RegionRebalanceAction;
import cs.bilkent.joker.engine.flow.PipelineId;
import cs.bilkent.joker.engine.flow.RegionExecPlan;
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

public class RegionExpanderTest extends AbstractJokerTest
{

    private static final int MAX_REPLICA_COUNT = 3;


    private final RegionExecPlan regionExecPlan = mock( RegionExecPlan.class );

    private final RegionExecPlan newRegionExecPlan = mock( RegionExecPlan.class );

    private final PipelineId pipelineId = new PipelineId( 0, 0 );

    private final PipelineMetrics bottleneckPipelineMetrics = mock( PipelineMetrics.class );

    private RegionExpander regionExpander = new RegionExpander( MAX_REPLICA_COUNT );


    @Before
    public void init ()
    {
        when( regionExecPlan.withNewReplicaCount( MAX_REPLICA_COUNT ) ).thenReturn( newRegionExecPlan );
    }

    @Test
    public void shouldNotExtendStatefulRegion ()
    {
        when( regionExecPlan.getRegionType() ).thenReturn( STATEFUL );

        final AdaptationAction action = regionExpander.resolve( regionExecPlan, bottleneckPipelineMetrics );

        assertNull( action );
    }

    @Test
    public void shouldNotExtendStatelessRegion ()
    {
        when( regionExecPlan.getRegionType() ).thenReturn( STATELESS );

        final AdaptationAction action = regionExpander.resolve( regionExecPlan, bottleneckPipelineMetrics );

        assertNull( action );
    }

    @Test
    public void shouldNotExtendIfMaxReplicaCountIsReached ()
    {
        when( regionExecPlan.getRegionType() ).thenReturn( PARTITIONED_STATEFUL );
        when( regionExecPlan.getReplicaCount() ).thenReturn( MAX_REPLICA_COUNT );

        final AdaptationAction action = regionExpander.resolve( regionExecPlan, bottleneckPipelineMetrics );

        assertNull( action );
    }

    @Test
    public void shouldExtendPartitionedStatefulRegion ()
    {
        when( regionExecPlan.getRegionType() ).thenReturn( PARTITIONED_STATEFUL );
        when( regionExecPlan.getReplicaCount() ).thenReturn( MAX_REPLICA_COUNT - 1 );

        final AdaptationAction action = regionExpander.resolve( regionExecPlan, bottleneckPipelineMetrics );

        assertTrue( action instanceof RegionRebalanceAction );
        final RegionRebalanceAction regionRebalanceAction = (RegionRebalanceAction) action;
        assertThat( regionRebalanceAction.getCurrentExecPlan(), equalTo( regionExecPlan ) );
        assertThat( regionRebalanceAction.getNewExecPlan(), equalTo( newRegionExecPlan ) );
    }

}
