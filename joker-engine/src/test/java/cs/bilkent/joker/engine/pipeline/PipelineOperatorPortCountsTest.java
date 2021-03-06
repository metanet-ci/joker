package cs.bilkent.joker.engine.pipeline;

import java.util.List;

import org.junit.Before;
import org.junit.Test;
import com.google.inject.Guice;
import com.google.inject.Injector;

import cs.bilkent.joker.JokerModule;
import cs.bilkent.joker.engine.config.JokerConfigBuilder;
import cs.bilkent.joker.engine.flow.RegionDef;
import cs.bilkent.joker.engine.flow.RegionExecPlan;
import static cs.bilkent.joker.engine.pipeline.OperatorReplicaStatus.RUNNING;
import cs.bilkent.joker.engine.region.FlowDefOptimizer;
import cs.bilkent.joker.engine.region.Region;
import cs.bilkent.joker.engine.region.RegionDefFormer;
import cs.bilkent.joker.engine.region.RegionManager;
import cs.bilkent.joker.flow.FlowDef;
import cs.bilkent.joker.flow.FlowDefBuilder;
import cs.bilkent.joker.operator.InitCtx;
import cs.bilkent.joker.operator.InvocationCtx;
import cs.bilkent.joker.operator.Operator;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.OperatorDefBuilder;
import cs.bilkent.joker.operator.scheduling.ScheduleWhenAvailable;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.AT_LEAST;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.scheduleWhenTuplesAvailableOnAny;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.scheduleWhenTuplesAvailableOnDefaultPort;
import cs.bilkent.joker.operator.scheduling.SchedulingStrategy;
import cs.bilkent.joker.operator.spec.OperatorSpec;
import static cs.bilkent.joker.operator.spec.OperatorType.STATEFUL;
import static cs.bilkent.joker.operator.spec.OperatorType.STATELESS;
import cs.bilkent.joker.operator.utils.Pair;
import cs.bilkent.joker.test.AbstractJokerTest;
import static java.util.Collections.singletonList;
import static java.util.stream.IntStream.range;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class PipelineOperatorPortCountsTest extends AbstractJokerTest
{

    private Injector injector;

    private RegionDefFormer regionDefFormer;

    private RegionManager regionManager;

    private FlowDefOptimizer flowDefOptimizer;

    @Before
    public void init ()
    {
        final JokerConfigBuilder configBuilder = new JokerConfigBuilder();
        configBuilder.getFlowDefOptimizerConfigBuilder().enableMergeRegions();

        injector = Guice.createInjector( new JokerModule( configBuilder.build() ) );
        regionDefFormer = injector.getInstance( RegionDefFormer.class );
        regionManager = injector.getInstance( RegionManager.class );
        flowDefOptimizer = injector.getInstance( FlowDefOptimizer.class );
    }

    @Test
    public void shouldInitializePipelineWhenUpstreamOperatorOutputPortsAreBiggerThanDownstreamOperatorInputPorts ()
    {
        final OperatorDef operator0 = OperatorDefBuilder.newInstance( "op0", StatelessUpstreamOperator.class )
                                                        .setInputPortCount( 0 )
                                                        .setOutputPortCount( 1 )
                                                        .build();

        final OperatorDef operator1 = OperatorDefBuilder.newInstance( "op1", StatelessUpstreamOperator.class )
                                                        .setInputPortCount( 1 )
                                                        .setOutputPortCount( 2 )
                                                        .build();

        final OperatorDef operator2 = OperatorDefBuilder.newInstance( "op2", StatefulDownstreamOperator.class ).setInputPortCount( 1 )
                                                        .build();

        final FlowDef flow = new FlowDefBuilder().add( operator0 ).add( operator1 ).add( operator2 ).connect( "op0", 0, "op1", 0 )
                                                 .connect( "op1", 0, "op2", 0 )
                                                 .connect( "op1", 1, "op2", 0 )
                                                 .build();

        testPipelineInitialization( flow );
    }

    @Test
    public void shouldInitializePipelineWhenUpstreamOperatorOutputPortsAreSmallerThanDownstreamOperatorInputPorts ()
    {
        final OperatorDef operator0 = OperatorDefBuilder.newInstance( "op0", StatelessUpstreamOperator.class )
                                                        .setInputPortCount( 0 )
                                                        .setOutputPortCount( 1 )
                                                        .build();

        final OperatorDef operator1 = OperatorDefBuilder.newInstance( "op1", StatelessUpstreamOperator.class )
                                                        .setInputPortCount( 1 )
                                                        .setOutputPortCount( 1 )
                                                        .build();

        final OperatorDef operator2 = OperatorDefBuilder.newInstance( "op2", StatefulDownstreamOperator.class ).setInputPortCount( 2 )
                                                        .build();

        final FlowDef flow = new FlowDefBuilder().add( operator0 ).add( operator1 ).add( operator2 ).connect( "op0", "op1" )
                                                 .connect( "op1", 0, "op2", 0 )
                                                 .connect( "op1", 0, "op2", 1 )
                                                 .build();

        testPipelineInitialization( flow );
    }

    private void testPipelineInitialization ( final FlowDef flow )
    {
        final Pair<FlowDef, List<RegionDef>> result = flowDefOptimizer.optimize( flow, regionDefFormer.createRegions( flow ) );
        final List<RegionDef> regionDefs = result._2;

        assertThat( regionDefs, hasSize( 2 ) );

        final RegionDef regionDef = regionDefs.get( 1 );
        final RegionExecPlan regionExecPlan = new RegionExecPlan( regionDef, singletonList( 0 ), 1 );

        final Region region = regionManager.createRegion( result._1, regionExecPlan );
        final PipelineReplica[] pipelineReplicas = region.getPipelineReplicas( 0 );
        final Pipeline pipeline = new Pipeline( pipelineReplicas[ 0 ].id().pipelineId, region );
        pipeline.init();

        assertThat( pipelineReplicas[ 0 ].getStatus(), equalTo( RUNNING ) );
    }

    @OperatorSpec( type = STATELESS )
    public static class StatelessUpstreamOperator implements Operator
    {

        @Override
        public SchedulingStrategy init ( final InitCtx ctx )
        {
            return ctx.getInputPortCount() == 0 ? ScheduleWhenAvailable.INSTANCE : scheduleWhenTuplesAvailableOnDefaultPort( 1 );
        }

        @Override
        public void invoke ( final InvocationCtx ctx )
        {

        }

    }


    @OperatorSpec( type = STATEFUL, outputPortCount = 0 )
    public static class StatefulDownstreamOperator implements Operator
    {

        @Override
        public SchedulingStrategy init ( final InitCtx ctx )
        {
            final int inputPortCount = ctx.getInputPortCount();
            return scheduleWhenTuplesAvailableOnAny( AT_LEAST, inputPortCount, 1, range( 0, inputPortCount ).toArray() );
        }

        @Override
        public void invoke ( final InvocationCtx ctx )
        {

        }

    }

}
