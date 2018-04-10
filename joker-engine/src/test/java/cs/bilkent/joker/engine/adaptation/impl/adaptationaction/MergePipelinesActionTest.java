package cs.bilkent.joker.engine.adaptation.impl.adaptationaction;

import java.util.List;

import org.junit.Before;
import org.junit.Test;

import cs.bilkent.joker.engine.adaptation.AdaptationAction;
import cs.bilkent.joker.engine.adaptation.AdaptationPerformer;
import static cs.bilkent.joker.engine.adaptation.impl.adaptationaction.RegionRebalanceActionTest.getRegion;
import cs.bilkent.joker.engine.flow.PipelineId;
import cs.bilkent.joker.engine.flow.RegionDef;
import cs.bilkent.joker.engine.flow.RegionExecPlan;
import cs.bilkent.joker.engine.region.RegionDefFormer;
import cs.bilkent.joker.engine.region.impl.IdGenerator;
import cs.bilkent.joker.engine.region.impl.PipelineTransformerImplTest.StatelessInput1Output1Operator;
import cs.bilkent.joker.engine.region.impl.RegionDefFormerImpl;
import cs.bilkent.joker.flow.FlowDef;
import cs.bilkent.joker.flow.FlowDefBuilder;
import cs.bilkent.joker.operator.InitCtx;
import cs.bilkent.joker.operator.InvocationCtx;
import cs.bilkent.joker.operator.Operator;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.OperatorDefBuilder;
import cs.bilkent.joker.operator.scheduling.ScheduleWhenAvailable;
import cs.bilkent.joker.operator.scheduling.SchedulingStrategy;
import cs.bilkent.joker.operator.schema.annotation.OperatorSchema;
import cs.bilkent.joker.operator.schema.annotation.PortSchema;
import static cs.bilkent.joker.operator.schema.annotation.PortSchemaScope.EXACT_FIELD_SET;
import cs.bilkent.joker.operator.schema.annotation.SchemaField;
import cs.bilkent.joker.operator.spec.OperatorSpec;
import static cs.bilkent.joker.operator.spec.OperatorType.STATEFUL;
import static cs.bilkent.joker.operator.spec.OperatorType.STATELESS;
import cs.bilkent.joker.test.AbstractJokerTest;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class MergePipelinesActionTest extends AbstractJokerTest
{

    private final OperatorDef source = OperatorDefBuilder.newInstance( "source", StatefulOperatorInput0Output1.class ).build();

    private final OperatorDef operator1 = OperatorDefBuilder.newInstance( "op1", StatelessInput1Output1Operator.class ).build();

    private final OperatorDef operator2 = OperatorDefBuilder.newInstance( "op2", StatelessInput1Output1Operator.class ).build();

    private final OperatorDef operator3 = OperatorDefBuilder.newInstance( "op3", StatelessInput1Output1Operator.class ).build();

    private final OperatorDef operator4 = OperatorDefBuilder.newInstance( "op4", StatelessInput1Output1Operator.class ).build();

    private final OperatorDef operator5 = OperatorDefBuilder.newInstance( "op5", StatelessInput1Output1Operator.class ).build();

    private final OperatorDef operator6 = OperatorDefBuilder.newInstance( "op6", StatelessInput1Output1Operator.class ).build();

    private final List<Integer> pipelineStartIndices = asList( 0, 3, 5 );

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
        final RegionDef region = getRegion( regions, STATELESS );
        regionExecPlan = new RegionExecPlan( region, pipelineStartIndices, 1 );
    }

    @Test
    public void shouldMergeAllPipelines ()
    {
        final MergePipelinesAction action = new MergePipelinesAction( regionExecPlan, regionExecPlan.getPipelineIds() );
        final RegionExecPlan newRegionExecPlan = action.getNewExecPlan();

        assertThat( action.getCurrentExecPlan(), equalTo( regionExecPlan ) );
        assertThat( newRegionExecPlan, equalTo( regionExecPlan.withMergedPipelines( pipelineStartIndices ) ) );
        assertThat( newRegionExecPlan.getPipelineIds(), equalTo( singletonList( regionExecPlan.getPipelineId( 0 ) ) ) );
    }

    @Test
    public void shouldMergePipelines ()
    {
        final List<PipelineId> mergedPipelineIds = asList( regionExecPlan.getPipelineId( 1 ), regionExecPlan.getPipelineId( 2 ) );
        final MergePipelinesAction action = new MergePipelinesAction( regionExecPlan, mergedPipelineIds );

        final RegionExecPlan newRegionExecPlan = action.getNewExecPlan();

        assertThat( newRegionExecPlan.getPipelineIds(),
                    equalTo( asList( regionExecPlan.getPipelineId( 0 ), regionExecPlan.getPipelineId( 1 ) ) ) );
    }

    @Test
    public void shouldRevertAction ()
    {
        final List<PipelineId> mergedPipelineIds = asList( regionExecPlan.getPipelineId( 1 ), regionExecPlan.getPipelineId( 2 ) );
        final MergePipelinesAction action = new MergePipelinesAction( regionExecPlan, mergedPipelineIds );
        final AdaptationAction revert = action.revert();

        assertThat( revert.getNewExecPlan(), equalTo( regionExecPlan ) );
        assertThat( revert.getCurrentExecPlan(),
                    equalTo( regionExecPlan.withMergedPipelines( asList( pipelineStartIndices.get( 1 ),
                                                                         pipelineStartIndices.get( 2 ) ) ) ) );
    }

    @Test
    public void shouldApplyAction ()
    {
        final List<PipelineId> mergedPipelineIds = asList( regionExecPlan.getPipelineId( 1 ), regionExecPlan.getPipelineId( 2 ) );
        final MergePipelinesAction action = new MergePipelinesAction( regionExecPlan, mergedPipelineIds );

        final AdaptationPerformer adaptationPerformer = mock( AdaptationPerformer.class );

        action.apply( adaptationPerformer );

        verify( adaptationPerformer ).mergePipelines( mergedPipelineIds );
    }

    @OperatorSpec( type = STATEFUL, inputPortCount = 0, outputPortCount = 1 )
    @OperatorSchema( outputs = { @PortSchema( portIndex = 0, scope = EXACT_FIELD_SET, fields = { @SchemaField( name = "field", type = Integer.class ) } ) } )
    public static class StatefulOperatorInput0Output1 implements Operator
    {

        @Override
        public SchedulingStrategy init ( final InitCtx ctx )
        {
            return ScheduleWhenAvailable.INSTANCE;
        }

        @Override
        public void invoke ( final InvocationCtx ctx )
        {

        }

    }

}
