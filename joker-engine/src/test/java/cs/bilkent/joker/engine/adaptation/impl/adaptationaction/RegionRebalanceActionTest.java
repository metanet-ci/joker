package cs.bilkent.joker.engine.adaptation.impl.adaptationaction;

import java.util.List;

import org.junit.Before;
import org.junit.Test;

import cs.bilkent.joker.engine.adaptation.AdaptationPerformer;
import cs.bilkent.joker.engine.flow.RegionDef;
import cs.bilkent.joker.engine.flow.RegionExecutionPlan;
import cs.bilkent.joker.engine.region.RegionDefFormer;
import cs.bilkent.joker.engine.region.impl.IdGenerator;
import cs.bilkent.joker.engine.region.impl.RegionDefFormerImpl;
import cs.bilkent.joker.engine.region.impl.RegionManagerImplTest.FlowExample6;
import cs.bilkent.joker.operator.spec.OperatorType;
import static cs.bilkent.joker.operator.spec.OperatorType.PARTITIONED_STATEFUL;
import cs.bilkent.joker.test.AbstractJokerTest;
import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class RegionRebalanceActionTest extends AbstractJokerTest
{

    private final int newReplicaCount = 4;

    private RegionDef region;

    private RegionExecutionPlan regionExecutionPlan;

    private RegionRebalanceAction action;

    @Before
    public void before ()
    {
        final FlowExample6 flowExample = new FlowExample6();

        final RegionDefFormer regionDefFormer = new RegionDefFormerImpl( new IdGenerator() );
        final List<RegionDef> regions = regionDefFormer.createRegions( flowExample.getFlow() );
        region = getRegion( regions, PARTITIONED_STATEFUL );
        assertTrue( region.getOperatorCount() > 1 );

        final List<Integer> pipelineStartIndices = asList( 0, region.getOperatorCount() / 2 );

        final int currentReplicaCount = 2;
        regionExecutionPlan = new RegionExecutionPlan( region, pipelineStartIndices, currentReplicaCount );
        action = new RegionRebalanceAction( regionExecutionPlan, newReplicaCount );
    }

    @Test
    public void shouldRebalanceRegion ()
    {
        assertThat( action.getCurrentRegionExecutionPlan(), equalTo( regionExecutionPlan ) );
        assertThat( action.getNewRegionExecutionPlan(), equalTo( regionExecutionPlan.withNewReplicaCount( newReplicaCount ) ) );
        assertThat( action.getNewRegionExecutionPlan().getPipelineIds(), equalTo( regionExecutionPlan.getPipelineIds() ) );
    }

    @Test
    public void shouldRevertRebalance ()
    {
        final RegionRebalanceAction revert = (RegionRebalanceAction) action.revert();

        assertThat( revert.getCurrentRegionExecutionPlan(), equalTo( regionExecutionPlan.withNewReplicaCount( newReplicaCount ) ) );
        assertThat( revert.getNewRegionExecutionPlan(), equalTo( regionExecutionPlan ) );
        assertThat( revert.getNewRegionExecutionPlan().getPipelineIds(), equalTo( regionExecutionPlan.getPipelineIds() ) );
    }

    @Test
    public void shouldApplyRebalance ()
    {
        final AdaptationPerformer adaptationPerformer = mock( AdaptationPerformer.class );

        action.apply( adaptationPerformer );

        verify( adaptationPerformer ).rebalanceRegion( regionExecutionPlan.getRegionId(), newReplicaCount );
    }

    public static RegionDef getRegion ( final List<RegionDef> regions, final OperatorType regionType )
    {
        return regions.stream().filter( r -> r.getRegionType() == regionType ).findFirst().orElse( null );
    }

}
