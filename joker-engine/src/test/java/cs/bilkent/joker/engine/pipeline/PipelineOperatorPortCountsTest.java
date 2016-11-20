package cs.bilkent.joker.engine.pipeline;

import java.util.List;

import org.junit.Before;
import org.junit.Test;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import cs.bilkent.joker.JokerModule;
import cs.bilkent.joker.engine.config.FlowDeploymentConfig;
import cs.bilkent.joker.engine.config.JokerConfig;
import cs.bilkent.joker.engine.region.FlowDeploymentDef;
import cs.bilkent.joker.engine.region.FlowDeploymentDefFormer;
import cs.bilkent.joker.engine.region.Region;
import cs.bilkent.joker.engine.region.RegionConfig;
import cs.bilkent.joker.engine.region.RegionDef;
import cs.bilkent.joker.engine.region.RegionDefFormer;
import cs.bilkent.joker.engine.region.RegionManager;
import cs.bilkent.joker.flow.FlowDef;
import cs.bilkent.joker.flow.FlowDefBuilder;
import cs.bilkent.joker.operator.InitializationContext;
import cs.bilkent.joker.operator.InvocationContext;
import cs.bilkent.joker.operator.Operator;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.OperatorDefBuilder;
import cs.bilkent.joker.operator.scheduling.ScheduleWhenAvailable;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.AT_LEAST;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.scheduleWhenTuplesAvailableOnAny;
import cs.bilkent.joker.operator.scheduling.SchedulingStrategy;
import cs.bilkent.joker.operator.spec.OperatorSpec;
import static cs.bilkent.joker.operator.spec.OperatorType.STATEFUL;
import static cs.bilkent.joker.operator.spec.OperatorType.STATELESS;
import cs.bilkent.joker.testutils.AbstractJokerTest;
import static java.lang.Boolean.TRUE;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class PipelineOperatorPortCountsTest extends AbstractJokerTest
{

    private Injector injector;

    private RegionDefFormer regionDefFormer;

    private RegionManager regionManager;

    private FlowDeploymentDefFormer flowDeploymentDefFormer;


    @Before
    public void init ()
    {
        final String configPath = JokerConfig.ENGINE_CONFIG_NAME + "." + FlowDeploymentConfig.CONFIG_NAME + "."
                                  + FlowDeploymentConfig.MERGE_STATELESS_REGIONS_WITH_STATEFUL_REGIONS;

        final Config flowDeploymentConfig = ConfigFactory.empty().withValue( configPath, ConfigValueFactory.fromAnyRef( TRUE ) );

        final Config config = ConfigFactory.load().withoutPath( configPath ).withFallback( flowDeploymentConfig );

        injector = Guice.createInjector( new JokerModule( new JokerConfig( config ) ) );
        regionDefFormer = injector.getInstance( RegionDefFormer.class );
        regionManager = injector.getInstance( RegionManager.class );
        flowDeploymentDefFormer = injector.getInstance( FlowDeploymentDefFormer.class );
    }

    @Test
    public void shouldInitializePipelineWhenUpstreamOperatorOutputPortsAreBiggerThanDownstreamOperatorInputPorts ()
    {
        final int upstreamOutputPortCount = 2;
        final int downstreamInputPortCount = 1;

        final OperatorDef operator1 = OperatorDefBuilder.newInstance( "op1", StatelessUpstreamOperator.class )
                                                        .setOutputPortCount( upstreamOutputPortCount )
                                                        .build();

        final OperatorDef operator2 = OperatorDefBuilder.newInstance( "op2", StatefulDownstreamOperator.class )
                                                        .setInputPortCount( downstreamInputPortCount )
                                                        .build();

        final FlowDef flow = new FlowDefBuilder().add( operator1 )
                                                 .add( operator2 )
                                                 .connect( "op1", 0, "op2", 0 )
                                                 .connect( "op1", 1, "op2", 0 )
                                                 .build();

        testPipelineInitialization( flow );
    }

    @Test
    public void shouldInitializePipelineWhenUpstreamOperatorOutputPortsAreSmallerThanDownstreamOperatorInputPorts ()
    {
        final int upstreamOutputPortCount = 1;
        final int downstreamInputPortCount = 2;

        final OperatorDef operator1 = OperatorDefBuilder.newInstance( "op1", StatelessUpstreamOperator.class )
                                                        .setOutputPortCount( upstreamOutputPortCount )
                                                        .build();

        final OperatorDef operator2 = OperatorDefBuilder.newInstance( "op2", StatefulDownstreamOperator.class )
                                                        .setInputPortCount( downstreamInputPortCount )
                                                        .build();

        final FlowDef flow = new FlowDefBuilder().add( operator1 )
                                                 .add( operator2 )
                                                 .connect( "op1", 0, "op2", 0 )
                                                 .connect( "op1", 0, "op2", 1 )
                                                 .build();

        testPipelineInitialization( flow );
    }

    private void testPipelineInitialization ( final FlowDef flow )
    {
        final FlowDeploymentDef flowDepDef = flowDeploymentDefFormer.createFlowDeploymentDef( flow, regionDefFormer.createRegions( flow ) );
        final List<RegionDef> regionDefs = flowDepDef.getRegions();
        assertThat( regionDefs, hasSize( 1 ) );

        final RegionDef regionDef = regionDefs.get( 0 );
        final RegionConfig regionConfig = new RegionConfig( regionDef, singletonList( 0 ), 1 );

        final Region region = regionManager.createRegion( flow, regionConfig );
        final PipelineReplica[] pipelineReplicas = region.getPipelineReplicas( 0 );
        final Pipeline pipeline = new Pipeline( pipelineReplicas[ 0 ].id().pipelineId, regionConfig, pipelineReplicas );
        pipeline.setUpstreamContext( new UpstreamContext( 0, new UpstreamConnectionStatus[] {} ) );
        pipeline.init();

        assertThat( pipelineReplicas[ 0 ].getStatus(), equalTo( OperatorReplicaStatus.RUNNING ) );
    }

    @OperatorSpec( type = STATELESS, inputPortCount = 0 )
    public static class StatelessUpstreamOperator implements Operator
    {

        @Override
        public SchedulingStrategy init ( final InitializationContext context )
        {
            return ScheduleWhenAvailable.INSTANCE;
        }

        @Override
        public void invoke ( final InvocationContext invocationContext )
        {

        }

    }


    @OperatorSpec( type = STATEFUL, outputPortCount = 0 )
    public static class StatefulDownstreamOperator implements Operator
    {

        @Override
        public SchedulingStrategy init ( final InitializationContext context )
        {
            final int inputPortCount = context.getInputPortCount();
            return scheduleWhenTuplesAvailableOnAny( AT_LEAST, inputPortCount, 1, 0 );
        }

        @Override
        public void invoke ( final InvocationContext invocationContext )
        {

        }

    }

}
