package cs.bilkent.joker.engine.adaptation.impl.adaptationaction;

import java.util.List;

import org.junit.Before;
import org.junit.Test;

import cs.bilkent.joker.engine.adaptation.AdaptationAction;
import cs.bilkent.joker.engine.adaptation.AdaptationPerformer;
import cs.bilkent.joker.engine.adaptation.impl.adaptationaction.MergePipelinesActionTest.StatefulOperatorInput0Output1;
import cs.bilkent.joker.engine.flow.PipelineId;
import cs.bilkent.joker.engine.flow.RegionDef;
import cs.bilkent.joker.engine.flow.RegionExecPlan;
import cs.bilkent.joker.engine.region.RegionDefFormer;
import cs.bilkent.joker.engine.region.impl.IdGenerator;
import cs.bilkent.joker.engine.region.impl.PipelineTransformerImplTest.StatelessInput1Output1Operator;
import cs.bilkent.joker.engine.region.impl.RegionDefFormerImpl;
import static cs.bilkent.joker.engine.region.impl.RegionExecPlanUtil.getPipelineStartIndicesToSplit;
import cs.bilkent.joker.flow.FlowDef;
import cs.bilkent.joker.flow.FlowDefBuilder;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.OperatorDefBuilder;
import static cs.bilkent.joker.operator.spec.OperatorType.STATELESS;
import cs.bilkent.joker.test.AbstractJokerTest;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class SplitPipelineActionTest extends AbstractJokerTest
{

    private final OperatorDef source = OperatorDefBuilder.newInstance( "source", StatefulOperatorInput0Output1.class ).build();

    private final OperatorDef operator1 = OperatorDefBuilder.newInstance( "op1", StatelessInput1Output1Operator.class ).build();

    private final OperatorDef operator2 = OperatorDefBuilder.newInstance( "op2", StatelessInput1Output1Operator.class ).build();

    private final OperatorDef operator3 = OperatorDefBuilder.newInstance( "op3", StatelessInput1Output1Operator.class ).build();

    private final OperatorDef operator4 = OperatorDefBuilder.newInstance( "op4", StatelessInput1Output1Operator.class ).build();

    private final OperatorDef operator5 = OperatorDefBuilder.newInstance( "op5", StatelessInput1Output1Operator.class ).build();

    private final OperatorDef operator6 = OperatorDefBuilder.newInstance( "op6", StatelessInput1Output1Operator.class ).build();

    private final List<Integer> pipelineStartIndices = asList( 0, 1, 5 );

    private RegionExecPlan regionExecPlan;


    @Before
    public void init ()
    {
        final FlowDef flow = new FlowDefBuilder().add( source )
                                                 .add( operator1 )
                                                 .add( operator2 )
                                                 .add( operator3 )
                                                 .add( operator4 )
                                                 .add( operator5 )
                                                 .add( operator6 )
                                                 .connect( source.getId(), operator1.getId() )
                                                 .connect( operator1.getId(), operator2.getId() )
                                                 .connect( operator2.getId(), operator3.getId() )
                                                 .connect( operator3.getId(), operator4.getId() )
                                                 .connect( operator4.getId(), operator5.getId() )
                                                 .connect( operator5.getId(), operator6.getId() )
                                                 .build();

        final RegionDefFormer regionDefFormer = new RegionDefFormerImpl( new IdGenerator() );
        final List<RegionDef> regions = regionDefFormer.createRegions( flow );
        final RegionDef region = RegionRebalanceActionTest.getRegion( regions, STATELESS );
        regionExecPlan = new RegionExecPlan( region, pipelineStartIndices, 1 );
    }

    @Test
    public void testSplitPipelines ()
    {
        final PipelineId splitPipelineId = regionExecPlan.getPipelineId( 1 );
        final List<Integer> pipelineOperatorIndices = singletonList( 3 );
        final SplitPipelineAction action = new SplitPipelineAction( regionExecPlan, splitPipelineId, pipelineOperatorIndices );
        final RegionExecPlan newRegionExecPlan = action.getNewExecPlan();

        assertThat( action.getCurrentExecPlan(), equalTo( regionExecPlan ) );
        assertThat( newRegionExecPlan,
                    equalTo( regionExecPlan.withSplitPipeline( getPipelineStartIndicesToSplit( regionExecPlan,
                                                                                               splitPipelineId,
                                                                                               pipelineOperatorIndices ) ) ) );

        final int regionId = regionExecPlan.getRegionId();
        assertThat( newRegionExecPlan.getPipelineIds(),
                    equalTo( asList( new PipelineId( regionId, 0 ),
                                     new PipelineId( regionId, 1 ),
                                     new PipelineId( regionId, 4 ),
                                     new PipelineId( regionId, 5 ) ) ) );
    }

    @Test
    public void testSplitPipelinesRevert ()
    {
        final PipelineId splitPipelineId = regionExecPlan.getPipelineId( 1 );
        final List<Integer> pipelineOperatorIndices = singletonList( 3 );
        final SplitPipelineAction action = new SplitPipelineAction( regionExecPlan, splitPipelineId, pipelineOperatorIndices );
        final AdaptationAction revert = action.revert();

        assertThat( revert.getCurrentExecPlan(),
                    equalTo( regionExecPlan.withSplitPipeline( getPipelineStartIndicesToSplit( regionExecPlan,
                                                                                               splitPipelineId,
                                                                                               pipelineOperatorIndices ) ) ) );
        assertThat( revert.getNewExecPlan(), equalTo( regionExecPlan ) );
    }

    @Test
    public void testSplitPipelinesActionApply ()
    {
        final PipelineId splitPipelineId = regionExecPlan.getPipelineId( 1 );
        final List<Integer> pipelineOperatorIndices = singletonList( 3 );
        final SplitPipelineAction action = new SplitPipelineAction( regionExecPlan, splitPipelineId, pipelineOperatorIndices );

        final AdaptationPerformer adaptationPerformer = mock( AdaptationPerformer.class );

        action.apply( adaptationPerformer );

        verify( adaptationPerformer ).splitPipeline( splitPipelineId, pipelineOperatorIndices );
    }

}
