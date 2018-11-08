package cs.bilkent.joker.engine.adaptation.impl;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import cs.bilkent.joker.engine.adaptation.AdaptationAction;
import cs.bilkent.joker.engine.config.JokerConfig;
import cs.bilkent.joker.engine.config.JokerConfigBuilder;
import cs.bilkent.joker.engine.flow.PipelineId;
import cs.bilkent.joker.engine.flow.RegionDef;
import cs.bilkent.joker.engine.flow.RegionExecPlan;
import cs.bilkent.joker.engine.metric.FlowMetrics;
import cs.bilkent.joker.engine.metric.LatencyMetrics;
import cs.bilkent.joker.engine.metric.LatencyMetrics.LatencyRecord;
import cs.bilkent.joker.engine.metric.LatencyMetricsHistory;
import cs.bilkent.joker.engine.metric.PipelineMetrics;
import cs.bilkent.joker.engine.metric.PipelineMetrics.PipelineMetricsBuilder;
import cs.bilkent.joker.engine.metric.PipelineMetricsHistory;
import cs.bilkent.joker.engine.region.RegionDefFormer;
import cs.bilkent.joker.engine.region.impl.IdGenerator;
import cs.bilkent.joker.engine.region.impl.PipelineTransformerImplTest.NopOperator;
import cs.bilkent.joker.engine.region.impl.RegionDefFormerImpl;
import cs.bilkent.joker.flow.FlowDef;
import cs.bilkent.joker.flow.FlowDefBuilder;
import cs.bilkent.joker.operator.InitCtx;
import cs.bilkent.joker.operator.InvocationCtx;
import cs.bilkent.joker.operator.Operator;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.OperatorDefBuilder;
import cs.bilkent.joker.operator.scheduling.ScheduleWhenAvailable;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.scheduleWhenTuplesAvailableOnDefaultPort;
import cs.bilkent.joker.operator.scheduling.SchedulingStrategy;
import cs.bilkent.joker.operator.schema.annotation.OperatorSchema;
import cs.bilkent.joker.operator.schema.annotation.PortSchema;
import static cs.bilkent.joker.operator.schema.annotation.PortSchemaScope.EXACT_FIELD_SET;
import cs.bilkent.joker.operator.schema.annotation.SchemaField;
import cs.bilkent.joker.operator.spec.OperatorSpec;
import cs.bilkent.joker.operator.spec.OperatorType;
import static cs.bilkent.joker.operator.spec.OperatorType.STATEFUL;
import static cs.bilkent.joker.operator.spec.OperatorType.STATELESS;
import cs.bilkent.joker.operator.utils.Pair;
import cs.bilkent.joker.test.AbstractJokerTest;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class LatencyOptimizingAdaptationManagerTest extends AbstractJokerTest
{

    private final OperatorDef operator1 = OperatorDefBuilder.newInstance( "op1", StatefulOperatorInput0Output1.class ).build();

    private final OperatorDef operator2 = OperatorDefBuilder.newInstance( "op2", PartitionedStatefulInput1Output1Operator.class )
                                                            .setPartitionFieldNames( singletonList( "field" ) )
                                                            .build();

    private final OperatorDef operator3 = OperatorDefBuilder.newInstance( "op3", PartitionedStatefulInput1Output1Operator.class )
                                                            .setPartitionFieldNames( singletonList( "field" ) )
                                                            .build();

    private final OperatorDef operator4 = OperatorDefBuilder.newInstance( "op4", StatelessInput1Output1Operator.class ).build();

    private final OperatorDef operator5 = OperatorDefBuilder.newInstance( "op5", StatelessInput1Output1Operator.class ).build();

    private JokerConfig config;

    private LatencyOptimizingAdaptationManager adaptationManager;

    private List<RegionExecPlan> regionExecPlans;

    @Before
    public void init ()
    {
        final JokerConfigBuilder configBuilder = new JokerConfigBuilder();
        configBuilder.getAdaptationConfigBuilder().enableAdaptation().setLatencyThresholdNanos( 100_000 );
        config = configBuilder.build();

        adaptationManager = new LatencyOptimizingAdaptationManager( config );

        final FlowDef flow = new FlowDefBuilder().add( operator1 )
                                                 .add( operator2 )
                                                 .add( operator3 )
                                                 .add( operator4 )
                                                 .add( operator5 )
                                                 .connect( "op1", "op2" )
                                                 .connect( "op2", "op3" )
                                                 .connect( "op3", "op4" )
                                                 .connect( "op4", "op5" )
                                                 .build();

        final RegionDefFormer regionDefFormer = new RegionDefFormerImpl( new IdGenerator() );
        final List<RegionDef> regionDefs = regionDefFormer.createRegions( flow );

        regionExecPlans = Arrays.asList( new RegionExecPlan( regionDefs.get( 0 ), Collections.singletonList( 0 ), 1 ),
                                         new RegionExecPlan( regionDefs.get( 1 ), Collections.singletonList( 0 ), 1 ),
                                         new RegionExecPlan( regionDefs.get( 2 ), Arrays.asList( 0, 1 ), 1 ) );

        adaptationManager.initialize( flow, regionExecPlans );
    }

    @Test
    public void test1 ()
    {
        final Map<Pair<String, Integer>, LatencyMetricsHistory> latencyMetricsHistories = createLatencyMetricsHistories( 20_000,
                                                                                                                         40_000,
                                                                                                                         99_999 );

        final FlowMetrics flowMetrics = new FlowMetrics( 1, createPipelineMetricsHistories(), latencyMetricsHistories );

        final List<AdaptationAction> actions = adaptationManager.adapt( regionExecPlans, flowMetrics );

        assertThat( actions, hasSize( 0 ) );
        assertThat( adaptationManager.getAdaptingRegion(), nullValue() );
        assertThat( adaptationManager.getAdaptingPipelineId(), nullValue() );
    }

    @Test
    public void test2 ()
    {
        final Map<Pair<String, Integer>, LatencyMetricsHistory> latencyMetricsHistories = createLatencyMetricsHistories( 40_000,
                                                                                                                         20_000,
                                                                                                                         100_001 );

        final FlowMetrics flowMetrics = new FlowMetrics( 1, createPipelineMetricsHistories(), latencyMetricsHistories );

        final List<AdaptationAction> actions = adaptationManager.adapt( regionExecPlans, flowMetrics );

        assertThat( actions, hasSize( 1 ) );
        assertThat( actions.get( 0 ).getCurrentExecPlan().getRegionId(), equalTo( 1 ) );
        assertThat( adaptationManager.getAdaptingRegion(), notNullValue() );
        assertThat( adaptationManager.getAdaptingRegion().getRegionId(), equalTo( 1 ) );
        assertThat( adaptationManager.getAdaptingPipelineId(), notNullValue() );
        assertThat( adaptationManager.getAdaptingPipelineId(), equalTo( new PipelineId( 1, 0 ) ) );
    }

    @Test
    public void test3 ()
    {
        final Map<Pair<String, Integer>, LatencyMetricsHistory> latencyMetricsHistories = createLatencyMetricsHistories( 20_000,
                                                                                                                         40_000,
                                                                                                                         100_001 );

        final FlowMetrics flowMetrics = new FlowMetrics( 1, createPipelineMetricsHistories(), latencyMetricsHistories );

        final List<AdaptationAction> actions = adaptationManager.adapt( regionExecPlans, flowMetrics );

        assertThat( actions, hasSize( 1 ) );
        assertThat( actions.get( 0 ).getCurrentExecPlan().getRegionId(), equalTo( 1 ) );
        assertThat( adaptationManager.getAdaptingRegion(), notNullValue() );
        assertThat( adaptationManager.getAdaptingRegion().getRegionId(), equalTo( 1 ) );
        assertThat( adaptationManager.getAdaptingPipelineId(), notNullValue() );
        assertThat( adaptationManager.getAdaptingPipelineId(), equalTo( new PipelineId( 1, 0 ) ) );
    }

    @Test
    public void test4 ()
    {
        final Map<Pair<String, Integer>, LatencyMetricsHistory> latencyMetricsHistories1 = createLatencyMetricsHistories( 40_000,
                                                                                                                          20_000,
                                                                                                                          100_002 );

        final FlowMetrics flowMetrics1 = new FlowMetrics( 1, createPipelineMetricsHistories(), latencyMetricsHistories1 );

        List<AdaptationAction> actions = adaptationManager.adapt( regionExecPlans, flowMetrics1 );

        assertThat( actions, hasSize( 1 ) );

        final Map<Pair<String, Integer>, LatencyMetricsHistory> latencyMetricsHistories2 = createLatencyMetricsHistories( 40_000,
                                                                                                                          20_000,
                                                                                                                          100_001 );

        final FlowMetrics flowMetrics2 = new FlowMetrics( 1, createPipelineMetricsHistories(), latencyMetricsHistories2 );

        actions = adaptationManager.adapt( regionExecPlans, flowMetrics2 );

        assertThat( actions, hasSize( 0 ) );

        assertThat( adaptationManager.getAdaptingRegion(), nullValue() );
        assertThat( adaptationManager.getAdaptingPipelineId(), nullValue() );
    }

    @Test
    public void test5 ()
    {
        final Map<Pair<String, Integer>, LatencyMetricsHistory> latencyMetricsHistories1 = createLatencyMetricsHistories( 40_000,
                                                                                                                          20_000,
                                                                                                                          100_002 );

        final FlowMetrics flowMetrics1 = new FlowMetrics( 1, createPipelineMetricsHistories(), latencyMetricsHistories1 );

        List<AdaptationAction> actions = adaptationManager.adapt( regionExecPlans, flowMetrics1 );

        assertThat( actions, hasSize( 1 ) );

        final Map<Pair<String, Integer>, LatencyMetricsHistory> latencyMetricsHistories2 = createLatencyMetricsHistories( 40_000,
                                                                                                                          20_000,
                                                                                                                          100_003 );

        final FlowMetrics flowMetrics2 = new FlowMetrics( 1, createPipelineMetricsHistories(), latencyMetricsHistories2 );

        actions = adaptationManager.adapt( regionExecPlans, flowMetrics2 );

        assertThat( actions, hasSize( 2 ) );

        assertThat( adaptationManager.getAdaptingRegion(), notNullValue() );
        assertThat( adaptationManager.getAdaptingRegion().getRegionId(), equalTo( 1 ) );
        assertThat( adaptationManager.getAdaptingPipelineId(), notNullValue() );
        assertThat( adaptationManager.getAdaptingPipelineId(), equalTo( new PipelineId( 1, 0 ) ) );
    }

    @Test
    public void test6 ()
    {
        final Map<Pair<String, Integer>, LatencyMetricsHistory> latencyMetricsHistories1 = createLatencyMetricsHistories( 40_000,
                                                                                                                          20_000,
                                                                                                                          100_002 );

        final FlowMetrics flowMetrics1 = new FlowMetrics( 1, createPipelineMetricsHistories(), latencyMetricsHistories1 );

        List<AdaptationAction> actions = adaptationManager.adapt( regionExecPlans, flowMetrics1 );

        assertThat( actions, hasSize( 1 ) );

        final Map<Pair<String, Integer>, LatencyMetricsHistory> latencyMetricsHistories2 = createLatencyMetricsHistories( 40_000,
                                                                                                                          20_000,
                                                                                                                          100_003 );

        final FlowMetrics flowMetrics2 = new FlowMetrics( 1, createPipelineMetricsHistories(), latencyMetricsHistories2 );

        actions = adaptationManager.adapt( regionExecPlans, flowMetrics2 );

        assertThat( actions, hasSize( 2 ) );

        final Map<Pair<String, Integer>, LatencyMetricsHistory> latencyMetricsHistories3 = createLatencyMetricsHistories( 40_000,
                                                                                                                          20_000,
                                                                                                                          100_004 );

        final FlowMetrics flowMetrics3 = new FlowMetrics( 1, createPipelineMetricsHistories(), latencyMetricsHistories3 );

        actions = adaptationManager.adapt( regionExecPlans, flowMetrics3 );

        assertThat( actions, hasSize( 1 ) );

        assertThat( adaptationManager.getAdaptingRegion(), nullValue() );
        assertThat( adaptationManager.getAdaptingPipelineId(), nullValue() );
        assertThat( adaptationManager.isAdaptationEnabled(), equalTo( false ) );
    }

    private Map<PipelineId, PipelineMetricsHistory> createPipelineMetricsHistories ()
    {
        final PipelineMetrics pipelineMetrics0 = new PipelineMetricsBuilder( regionExecPlans.get( 0 ).getPipelineId( 0 ),
                                                                             1,
                                                                             1,
                                                                             1,
                                                                             0 ).setPipelineCost( 0, 0.75 )
                                                                                .setOperatorCost( 0, 0, 0.25 )
                                                                                .setCpuUtilizationRatio( 0, 0.5 )
                                                                                .build();

        final PipelineMetrics pipelineMetrics1 = new PipelineMetricsBuilder( regionExecPlans.get( 1 ).getPipelineId( 0 ),
                                                                             1,
                                                                             1,
                                                                             2,
                                                                             1 ).setPipelineCost( 0, 0.1 )
                                                                                .setOperatorCost( 0, 0, 0.4 )
                                                                                .setOperatorCost( 0, 1, 0.5 )
                                                                                .setInboundThroughput( 0, 0, 100 )
                                                                                .setCpuUtilizationRatio( 0, 0.5 )
                                                                                .build();

        final PipelineMetrics pipelineMetrics2 = new PipelineMetricsBuilder( regionExecPlans.get( 2 ).getPipelineId( 0 ),
                                                                             1,
                                                                             1,
                                                                             2,
                                                                             1 ).setPipelineCost( 0, 0.1 )
                                                                                .setOperatorCost( 0, 0, 0.4 )
                                                                                .setOperatorCost( 0, 1, 0.5 )
                                                                                .setInboundThroughput( 0, 0, 100 )
                                                                                .setCpuUtilizationRatio( 0, 0.5 )
                                                                                .build();

        final PipelineMetricsHistory pipelineMetricsHistory0 = new PipelineMetricsHistory( pipelineMetrics0, 1 );
        final PipelineMetricsHistory pipelineMetricsHistory1 = new PipelineMetricsHistory( pipelineMetrics1, 1 );
        final PipelineMetricsHistory pipelineMetricsHistory2 = new PipelineMetricsHistory( pipelineMetrics2, 1 );

        final Map<PipelineId, PipelineMetricsHistory> pipelineMetricsHistories = new HashMap<>();
        pipelineMetricsHistories.put( pipelineMetricsHistory0.getPipelineId(), pipelineMetricsHistory0 );
        pipelineMetricsHistories.put( pipelineMetricsHistory1.getPipelineId(), pipelineMetricsHistory1 );
        pipelineMetricsHistories.put( pipelineMetricsHistory2.getPipelineId(), pipelineMetricsHistory2 );
        return pipelineMetricsHistories;
    }

    private Map<String, LatencyRecord> createInvocationLatencies ()
    {
        final Map<String, LatencyRecord> invocationLatencies = new HashMap<>();
        invocationLatencies.put( operator1.getId(), newLatencyRecord( 10_000 ) );
        invocationLatencies.put( operator2.getId(), newLatencyRecord( 10_000 ) );
        invocationLatencies.put( operator3.getId(), newLatencyRecord( 10_000 ) );
        invocationLatencies.put( operator4.getId(), newLatencyRecord( 10_000 ) );
        invocationLatencies.put( operator5.getId(), newLatencyRecord( 10_000 ) );
        return invocationLatencies;
    }

    private Map<String, LatencyRecord> createQueueLatencies ( final long operator2Latency, final long operator4Latency )
    {
        final Map<String, LatencyRecord> queueLatencies = new HashMap<>();
        queueLatencies.put( operator2.getId(), newLatencyRecord( operator2Latency ) );
        queueLatencies.put( operator4.getId(), newLatencyRecord( operator4Latency ) );

        return queueLatencies;
    }

    private Map<Pair<String, Integer>, LatencyMetricsHistory> createLatencyMetricsHistories ( final long operator2Latency,
                                                                                              final long operator4Latency,
                                                                                              final long tupleLatency )
    {
        final Map<String, LatencyRecord> queueLatencies = createQueueLatencies( operator2Latency, operator4Latency );

        final LatencyMetrics latencyMetrics = new LatencyMetrics( operator5.getId(),
                                                                  0,
                                                                  1,
                                                                  newLatencyRecord( tupleLatency ),
                                                                  createInvocationLatencies(),
                                                                  queueLatencies );

        final LatencyMetricsHistory latencyMetricsHistory = new LatencyMetricsHistory( latencyMetrics, 1 );
        final Map<Pair<String, Integer>, LatencyMetricsHistory> latencyMetricsHistories = new HashMap<>();
        latencyMetricsHistories.put( Pair.of( operator5.getId(), 0 ), latencyMetricsHistory );

        return latencyMetricsHistories;
    }

    private LatencyRecord newLatencyRecord ( long mean )
    {
        return new LatencyRecord( mean, 0, 0, 0, 0, 0, 0, 0, 0 );
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


    @OperatorSpec( type = STATELESS, inputPortCount = 1, outputPortCount = 1 )
    public static class StatelessInput1Output1Operator extends NopOperator
    {

        @Override
        public SchedulingStrategy init ( final InitCtx ctx )
        {
            return scheduleWhenTuplesAvailableOnDefaultPort( 1 );
        }

    }


    @OperatorSpec( type = OperatorType.PARTITIONED_STATEFUL, inputPortCount = 1, outputPortCount = 1 )
    @OperatorSchema( inputs = { @PortSchema( portIndex = 0, scope = EXACT_FIELD_SET, fields = { @SchemaField( name = "field", type = Integer.class ) } ) }, outputs = {
            @PortSchema( portIndex = 0, scope = EXACT_FIELD_SET, fields = { @SchemaField( name = "field", type = Integer.class ) } ) } )
    public static class PartitionedStatefulInput1Output1Operator extends NopOperator
    {

        @Override
        public SchedulingStrategy init ( final InitCtx ctx )
        {
            return scheduleWhenTuplesAvailableOnDefaultPort( 1 );
        }

    }

}
