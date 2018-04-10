package cs.bilkent.joker.engine.pipeline.impl;

import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import org.junit.Before;
import org.junit.Test;
import com.google.inject.Guice;
import com.google.inject.Injector;

import cs.bilkent.joker.JokerModule;
import cs.bilkent.joker.engine.config.JokerConfig;
import cs.bilkent.joker.engine.flow.RegionDef;
import cs.bilkent.joker.engine.flow.RegionExecPlan;
import cs.bilkent.joker.engine.pipeline.DownstreamCollector;
import static cs.bilkent.joker.engine.pipeline.OperatorReplicaStatus.INITIAL;
import cs.bilkent.joker.engine.pipeline.Pipeline;
import cs.bilkent.joker.engine.pipeline.PipelineManager;
import cs.bilkent.joker.engine.pipeline.impl.PipelineManagerImpl.IngestionTimeInjector;
import cs.bilkent.joker.engine.pipeline.impl.PipelineManagerImpl.LatencyRecorder;
import cs.bilkent.joker.engine.pipeline.impl.downstreamcollector.CompositeDownstreamCollector;
import cs.bilkent.joker.engine.pipeline.impl.downstreamcollector.DownstreamCollector1;
import cs.bilkent.joker.engine.pipeline.impl.downstreamcollector.PartitionedDownstreamCollector1;
import cs.bilkent.joker.engine.region.RegionDefFormer;
import cs.bilkent.joker.engine.tuplequeue.OperatorQueue;
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
import cs.bilkent.joker.operator.schema.annotation.OperatorSchema;
import cs.bilkent.joker.operator.schema.annotation.PortSchema;
import static cs.bilkent.joker.operator.schema.annotation.PortSchemaScope.EXACT_FIELD_SET;
import cs.bilkent.joker.operator.schema.annotation.SchemaField;
import cs.bilkent.joker.operator.spec.OperatorSpec;
import static cs.bilkent.joker.operator.spec.OperatorType.PARTITIONED_STATEFUL;
import static cs.bilkent.joker.operator.spec.OperatorType.STATEFUL;
import static cs.bilkent.joker.operator.spec.OperatorType.STATELESS;
import cs.bilkent.joker.test.AbstractJokerTest;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class PipelineManagerImplTest extends AbstractJokerTest
{

    private final JokerConfig jokerConfig = new JokerConfig();

    private RegionDefFormer regionDefFormer;

    private PipelineManagerImpl pipelineManager;

    @Before
    public void init ()
    {
        final Injector injector = Guice.createInjector( new JokerModule( jokerConfig ) );
        regionDefFormer = injector.getInstance( RegionDefFormer.class );
        pipelineManager = (PipelineManagerImpl) injector.getInstance( PipelineManager.class );
    }

    @Test
    public void test_singleStatefulRegion ()
    {
        final OperatorDef operatorDef = OperatorDefBuilder.newInstance( "op1", StatefulOperatorInput0Output1.class ).build();
        final FlowDef flow = new FlowDefBuilder().add( operatorDef ).build();

        final List<RegionDef> regions = regionDefFormer.createRegions( flow );
        final RegionExecPlan regionExecPlan = new RegionExecPlan( regions.get( 0 ), singletonList( 0 ), 1 );

        final List<Pipeline> pipelines = pipelineManager.createPipelines( flow, singletonList( regionExecPlan ) );

        assertEquals( 1, pipelines.size() );

        final Pipeline pipeline = pipelines.get( 0 );
        assertEquals( 0, pipeline.getOperatorIndex( operatorDef ) );
        assertEquals( INITIAL, pipeline.getPipelineStatus() );

        assertNotNull( pipeline.getPipelineReplica( 0 ) );
        assertNotNull( pipeline.getDownstreamCollector( 0 ) );
    }

    @Test
    public void test_partitionedStatefulRegion_statefulRegion ()
    {
        final OperatorDef operatorDef0 = OperatorDefBuilder.newInstance( "op0", StatefulOperatorInput0Output1.class ).build();
        final OperatorDef operatorDef1 = OperatorDefBuilder.newInstance( "op1", PartitionedStatefulOperatorInput2Output2.class )
                                                           .setPartitionFieldNames( singletonList( "field1" ) )
                                                           .build();
        final OperatorDef operatorDef2 = OperatorDefBuilder.newInstance( "op2", StatefulOperatorInput1Output1.class ).build();
        final FlowDef flow = new FlowDefBuilder().add( operatorDef0 )
                                                 .add( operatorDef1 )
                                                 .add( operatorDef2 )
                                                 .connect( "op0", "op1" )
                                                 .connect( "op1", "op2" )
                                                 .build();
        final List<RegionDef> regions = regionDefFormer.createRegions( flow );
        assertEquals( 3, regions.size() );
        final RegionDef statefulRegionDef0 = findRegion( regions, operatorDef0 );
        final RegionDef partitionedStatefulRegionDef = findRegion( regions, operatorDef1 );
        final RegionDef statefulRegionDef1 = findRegion( regions, operatorDef2 );
        final RegionExecPlan regionExecPlan0 = new RegionExecPlan( statefulRegionDef0, singletonList( 0 ), 1 );
        final RegionExecPlan regionExecPlan1 = new RegionExecPlan( partitionedStatefulRegionDef, singletonList( 0 ), 2 );
        final RegionExecPlan regionExecPlan2 = new RegionExecPlan( statefulRegionDef1, singletonList( 0 ), 1 );
        final List<Pipeline> pipelines = pipelineManager.createPipelines( flow,
                                                                          asList( regionExecPlan0, regionExecPlan1, regionExecPlan2 ) );

        assertEquals( 3, pipelines.size() );

        final Pipeline pipeline1 = pipelines.get( 1 );
        assertEquals( partitionedStatefulRegionDef, pipeline1.getRegionDef() );
        assertEquals( 0, pipeline1.getOperatorIndex( operatorDef1 ) );
        assertNotEquals( pipeline1.getPipelineReplica( 0 ), pipeline1.getPipelineReplica( 1 ) );
        assertTrue( pipeline1.getDownstreamCollector( 0 ) instanceof DownstreamCollector1 );
        assertTrue( pipeline1.getDownstreamCollector( 1 ) instanceof DownstreamCollector1 );
        assertEquals( INITIAL, pipeline1.getPipelineStatus() );

        final Pipeline pipeline2 = pipelines.get( 2 );
        assertEquals( INITIAL, pipeline2.getPipelineStatus() );
        assertEquals( ( (Supplier<OperatorQueue>) pipeline1.getDownstreamCollector( 0 ) ).get(),
                      pipeline2.getPipelineReplica( 0 ).getEffectiveQueue() );
        assertEquals( ( (Supplier<OperatorQueue>) pipeline1.getDownstreamCollector( 1 ) ).get(),
                      pipeline2.getPipelineReplica( 0 ).getEffectiveQueue() );
    }

    @Test
    public void test_partitionedStatefulRegion_statefulRegionAndStatelessRegion ()
    {
        final OperatorDef operatorDef0 = OperatorDefBuilder.newInstance( "op0", StatefulOperatorInput0Output1.class ).build();
        final OperatorDef operatorDef1 = OperatorDefBuilder.newInstance( "op1", PartitionedStatefulOperatorInput2Output2.class )
                                                           .setPartitionFieldNames( singletonList( "field1" ) )
                                                           .build();
        final OperatorDef operatorDef2 = OperatorDefBuilder.newInstance( "op2", StatefulOperatorInput1Output1.class ).build();
        final OperatorDef operatorDef3 = OperatorDefBuilder.newInstance( "op3", StatelessOperatorInput1Output1.class ).build();
        final FlowDef flow = new FlowDefBuilder().add( operatorDef0 )
                                                 .add( operatorDef1 )
                                                 .add( operatorDef2 )
                                                 .add( operatorDef3 )
                                                 .connect( "op0", "op1" )
                                                 .connect( "op1", "op2" )
                                                 .connect( "op1", "op3" )
                                                 .build();
        final List<RegionDef> regions = regionDefFormer.createRegions( flow );
        assertEquals( 4, regions.size() );

        final RegionDef statefulRegionDef0 = findRegion( regions, operatorDef0 );
        final RegionDef partitionedStatefulRegionDef = findRegion( regions, operatorDef1 );
        final RegionDef statefulRegionDef1 = findRegion( regions, operatorDef2 );
        final RegionDef statelessRegionDef = findRegion( regions, operatorDef3 );
        final RegionExecPlan regionExecPlan0 = new RegionExecPlan( statefulRegionDef0, singletonList( 0 ), 1 );
        final RegionExecPlan regionExecPlan1 = new RegionExecPlan( partitionedStatefulRegionDef, singletonList( 0 ), 2 );
        final RegionExecPlan regionExecPlan2 = new RegionExecPlan( statefulRegionDef1, singletonList( 0 ), 1 );
        final RegionExecPlan regionExecPlan3 = new RegionExecPlan( statelessRegionDef, singletonList( 0 ), 1 );
        final List<Pipeline> pipelines = pipelineManager.createPipelines( flow,
                                                                          asList( regionExecPlan0,
                                                                                  regionExecPlan1,
                                                                                  regionExecPlan2,
                                                                                  regionExecPlan3 ) );

        assertEquals( 4, pipelines.size() );

        final Pipeline pipeline0 = pipelines.get( 0 );
        final Pipeline pipeline1 = pipelines.get( 1 );
        final Pipeline pipeline2 = pipelines.get( 2 );
        final Pipeline pipeline3 = pipelines.get( 3 );

        assertEquals( INITIAL, pipeline0.getPipelineStatus() );
        assertEquals( INITIAL, pipeline1.getPipelineStatus() );
        assertEquals( INITIAL, pipeline2.getPipelineStatus() );
        assertEquals( INITIAL, pipeline3.getPipelineStatus() );

        assertEquals( statefulRegionDef0, pipeline0.getRegionDef() );
        assertEquals( 0, pipeline0.getOperatorIndex( operatorDef0 ) );

        assertEquals( partitionedStatefulRegionDef, pipeline1.getRegionDef() );
        assertEquals( 0, pipeline1.getOperatorIndex( operatorDef1 ) );
        assertNotEquals( pipeline1.getPipelineReplica( 0 ), pipeline1.getPipelineReplica( 1 ) );

        assertTrue( pipeline1.getDownstreamCollector( 0 ) instanceof CompositeDownstreamCollector );
        assertTrue( pipeline1.getDownstreamCollector( 1 ) instanceof CompositeDownstreamCollector );
        final DownstreamCollector[] collectors0 = ( (CompositeDownstreamCollector) pipeline1.getDownstreamCollector( 0 ) )
                                                          .getDownstreamCollectors();

        final DownstreamCollector[] collectors1 = ( (CompositeDownstreamCollector) pipeline1.getDownstreamCollector( 1 ) )
                                                          .getDownstreamCollectors();

        if ( ( (Supplier<OperatorQueue>) collectors0[ 0 ] ).get().equals( pipeline2.getPipelineReplica( 0 ).getEffectiveQueue() ) )
        {
            assertEquals( ( (Supplier<OperatorQueue>) collectors0[ 1 ] ).get(), pipeline3.getPipelineReplica( 0 ).getEffectiveQueue() );
        }
        else if ( ( (Supplier<OperatorQueue>) collectors0[ 1 ] ).get().equals( pipeline2.getPipelineReplica( 0 ).getEffectiveQueue() ) )
        {
            assertEquals( ( (Supplier<OperatorQueue>) collectors0[ 0 ] ).get(), pipeline3.getPipelineReplica( 0 ).getEffectiveQueue() );
        }
        else
        {
            fail();
        }

        if ( ( (Supplier<OperatorQueue>) collectors1[ 0 ] ).get().equals( pipeline2.getPipelineReplica( 0 ).getEffectiveQueue() ) )
        {
            assertEquals( ( (Supplier<OperatorQueue>) collectors1[ 1 ] ).get(), pipeline3.getPipelineReplica( 0 ).getEffectiveQueue() );
        }
        else if ( ( (Supplier<OperatorQueue>) collectors1[ 1 ] ).get().equals( pipeline2.getPipelineReplica( 0 ).getEffectiveQueue() ) )
        {
            assertEquals( ( (Supplier<OperatorQueue>) collectors1[ 0 ] ).get(), pipeline3.getPipelineReplica( 0 ).getEffectiveQueue() );
        }
        else
        {
            fail();
        }
    }

    @Test
    public void test_statefulRegion_partitionedStatefulRegionWithPartitionedStatefulAndStatelessOperators ()
    {
        final OperatorDef operatorDef1 = OperatorDefBuilder.newInstance( "op1", StatefulOperatorInput0Output1.class ).build();
        final OperatorDef operatorDef2 = OperatorDefBuilder.newInstance( "op2", PartitionedStatefulOperatorInput2Output2.class )
                                                           .setPartitionFieldNames( singletonList( "field1" ) )
                                                           .build();
        final OperatorDef operatorDef3 = OperatorDefBuilder.newInstance( "op3", StatelessOperatorInput1Output1.class ).build();
        final FlowDef flow = new FlowDefBuilder().add( operatorDef1 )
                                                 .add( operatorDef2 )
                                                 .add( operatorDef3 )
                                                 .connect( "op1", "op2" )
                                                 .connect( "op2", "op3" )
                                                 .build();

        final List<RegionDef> regions = regionDefFormer.createRegions( flow );
        assertEquals( 2, regions.size() );
        final RegionDef statefulRegionDef = findRegion( regions, operatorDef1 );
        final RegionDef partitionedStatefulRegionDef = findRegion( regions, operatorDef2 );
        final RegionExecPlan regionExecPlan1 = new RegionExecPlan( statefulRegionDef, singletonList( 0 ), 1 );
        final RegionExecPlan regionExecPlan2 = new RegionExecPlan( partitionedStatefulRegionDef, asList( 0, 1 ), 2 );

        final List<Pipeline> pipelines = pipelineManager.createPipelines( flow, asList( regionExecPlan1, regionExecPlan2 ) );

        assertEquals( 3, pipelines.size() );

        final Pipeline pipeline1 = pipelines.get( 0 );
        final Pipeline pipeline2 = pipelines.get( 1 );
        final Pipeline pipeline3 = pipelines.get( 2 );

        assertEquals( INITIAL, pipeline1.getPipelineStatus() );
        assertEquals( INITIAL, pipeline2.getPipelineStatus() );
        assertEquals( INITIAL, pipeline3.getPipelineStatus() );

        assertEquals( statefulRegionDef, pipeline1.getRegionDef() );
        assertEquals( 0, pipeline1.getOperatorIndex( operatorDef1 ) );
        final DownstreamCollector pipeline1Downstream = pipeline1.getDownstreamCollector( 0 );
        assertTrue( pipeline1Downstream instanceof IngestionTimeInjector );
        assertTrue( ( (IngestionTimeInjector) pipeline1Downstream ).getDownstream() instanceof PartitionedDownstreamCollector1 );
        assertArrayEquals( ( (Supplier<OperatorQueue[]>) ( (IngestionTimeInjector) pipeline1Downstream ).getDownstream() ).get(),
                           new OperatorQueue[] { pipeline2.getPipelineReplica( 0 ).getEffectiveQueue(),
                                                 pipeline2.getPipelineReplica( 1 ).getEffectiveQueue() } );

        assertEquals( partitionedStatefulRegionDef, pipeline2.getRegionDef() );
        assertEquals( 0, pipeline2.getOperatorIndex( operatorDef2 ) );
        assertFalse( pipeline2.getPipelineReplica( 0 ) == pipeline2.getPipelineReplica( 1 ) );
        assertTrue( pipeline2.getDownstreamCollector( 0 ) instanceof DownstreamCollector1 );
        assertTrue( pipeline2.getDownstreamCollector( 1 ) instanceof DownstreamCollector1 );
        assertEquals( ( (Supplier<OperatorQueue>) pipeline2.getDownstreamCollector( 0 ) ).get(),
                      pipeline3.getPipelineReplica( 0 ).getEffectiveQueue() );
        assertEquals( ( (Supplier<OperatorQueue>) pipeline2.getDownstreamCollector( 1 ) ).get(),
                      pipeline3.getPipelineReplica( 1 ).getEffectiveQueue() );

        assertEquals( partitionedStatefulRegionDef, pipeline3.getRegionDef() );
        assertEquals( 0, pipeline3.getOperatorIndex( operatorDef3 ) );
        assertFalse( pipeline3.getPipelineReplica( 0 ) == pipeline3.getPipelineReplica( 1 ) );
        assertTrue( pipeline3.getDownstreamCollector( 0 ) instanceof LatencyRecorder );
    }

    @Test
    public void test_statefulRegion_partitionedStatefulRegionWithStatelessAndPartitionedStatefulOperators ()
    {
        final OperatorDef operatorDef1 = OperatorDefBuilder.newInstance( "op1", StatefulOperatorInput0Output1.class ).build();
        final OperatorDef operatorDef2 = OperatorDefBuilder.newInstance( "op2", StatelessOperatorInput1Output1.class ).build();
        final OperatorDef operatorDef3 = OperatorDefBuilder.newInstance( "op3", PartitionedStatefulOperatorInput2Output2.class )
                                                           .setPartitionFieldNames( singletonList( "field1" ) )
                                                           .build();

        final FlowDef flow = new FlowDefBuilder().add( operatorDef1 )
                                                 .add( operatorDef2 )
                                                 .add( operatorDef3 )
                                                 .connect( "op1", "op2" )
                                                 .connect( "op2", "op3" )
                                                 .build();

        final List<RegionDef> regions = regionDefFormer.createRegions( flow );
        assertEquals( 2, regions.size() );

        final RegionDef statefulRegionDef = findRegion( regions, operatorDef1 );
        final RegionDef partitionedStatefulRegionDef = findRegion( regions, operatorDef3 );
        final RegionExecPlan regionExecPlan1 = new RegionExecPlan( statefulRegionDef, singletonList( 0 ), 1 );
        final RegionExecPlan regionExecPlan2 = new RegionExecPlan( partitionedStatefulRegionDef, asList( 0, 1 ), 2 );

        final List<Pipeline> pipelines = pipelineManager.createPipelines( flow, asList( regionExecPlan1, regionExecPlan2 ) );

        assertEquals( 3, pipelines.size() );

        final Pipeline pipeline1 = pipelines.get( 0 );
        final Pipeline pipeline2 = pipelines.get( 1 );
        final Pipeline pipeline3 = pipelines.get( 2 );

        assertEquals( INITIAL, pipeline1.getPipelineStatus() );
        assertEquals( INITIAL, pipeline2.getPipelineStatus() );
        assertEquals( INITIAL, pipeline3.getPipelineStatus() );

        assertEquals( statefulRegionDef, pipeline1.getRegionDef() );
        assertEquals( 0, pipeline1.getOperatorIndex( operatorDef1 ) );
        final DownstreamCollector sourceDownstream = pipeline1.getDownstreamCollector( 0 );
        assertTrue( sourceDownstream instanceof IngestionTimeInjector );
        assertTrue( ( (IngestionTimeInjector) sourceDownstream ).getDownstream() instanceof PartitionedDownstreamCollector1 );
        final PartitionedDownstreamCollector1 partitionedDownstream = (PartitionedDownstreamCollector1) ( (IngestionTimeInjector)
                                                                                                                  sourceDownstream )
                                                                                                                .getDownstream();
        assertArrayEquals( ( partitionedDownstream ).get(),
                           new OperatorQueue[] { pipeline2.getPipelineReplica( 0 ).getEffectiveQueue(),
                                                 pipeline2.getPipelineReplica( 1 ).getEffectiveQueue() } );

        assertEquals( partitionedStatefulRegionDef, pipeline2.getRegionDef() );
        assertEquals( 0, pipeline2.getOperatorIndex( operatorDef2 ) );
        assertFalse( pipeline2.getPipelineReplica( 0 ) == pipeline2.getPipelineReplica( 1 ) );
        assertTrue( pipeline2.getDownstreamCollector( 0 ) instanceof DownstreamCollector1 );
        assertTrue( pipeline2.getDownstreamCollector( 1 ) instanceof DownstreamCollector1 );
        assertEquals( ( (Supplier<OperatorQueue>) pipeline2.getDownstreamCollector( 0 ) ).get(),
                      pipeline3.getPipelineReplica( 0 ).getEffectiveQueue() );
        assertEquals( ( (Supplier<OperatorQueue>) pipeline2.getDownstreamCollector( 1 ) ).get(),
                      pipeline3.getPipelineReplica( 1 ).getEffectiveQueue() );

        assertEquals( partitionedStatefulRegionDef, pipeline3.getRegionDef() );
        assertEquals( 0, pipeline3.getOperatorIndex( operatorDef3 ) );
        assertFalse( pipeline3.getPipelineReplica( 0 ) == pipeline3.getPipelineReplica( 1 ) );
        assertTrue( pipeline3.getDownstreamCollector( 0 ) instanceof LatencyRecorder );
    }

    @Test
    public void test_twoRegions_connectedToSingleRegion ()
    {
        final OperatorDef operatorDef1 = OperatorDefBuilder.newInstance( "op1", StatefulOperatorInput0Output1.class ).build();
        final OperatorDef operatorDef2 = OperatorDefBuilder.newInstance( "op2", StatefulOperatorInput0Output1.class ).build();
        final OperatorDef operatorDef3 = OperatorDefBuilder.newInstance( "op3", StatelessOperatorInput1Output1.class ).build();

        final FlowDef flow = new FlowDefBuilder().add( operatorDef1 )
                                                 .add( operatorDef2 )
                                                 .add( operatorDef3 )
                                                 .connect( "op1", "op3" )
                                                 .connect( "op2", "op3" )
                                                 .build();

        final List<RegionDef> regions = regionDefFormer.createRegions( flow );
        assertEquals( 3, regions.size() );

        final RegionDef statelessRegionDef = findRegion( regions, operatorDef3 );

        final List<RegionDef> statefulRegionDefs = regions.stream()
                                                          .filter( regionDef -> regionDef.getRegionType() == STATEFUL )
                                                          .collect( toList() );

        final RegionDef statefulRegionDef1 = statefulRegionDefs.get( 0 ), statefulRegionDef2 = statefulRegionDefs.get( 1 );

        final RegionExecPlan regionExecPlan1 = new RegionExecPlan( statefulRegionDef1, singletonList( 0 ), 1 );
        final RegionExecPlan regionExecPlan2 = new RegionExecPlan( statefulRegionDef2, singletonList( 0 ), 1 );
        final RegionExecPlan regionExecPlan3 = new RegionExecPlan( statelessRegionDef, singletonList( 0 ), 1 );

        final List<Pipeline> pipelines = pipelineManager.createPipelines( flow,
                                                                          asList( regionExecPlan1, regionExecPlan2, regionExecPlan3 ) );

        final Pipeline pipeline1 = pipelines.get( 0 );
        final Pipeline pipeline2 = pipelines.get( 1 );
        final Pipeline pipeline3 = pipelines.get( 2 );

        assertEquals( INITIAL, pipeline1.getPipelineStatus() );
        assertEquals( INITIAL, pipeline2.getPipelineStatus() );
        assertEquals( INITIAL, pipeline3.getPipelineStatus() );

        assertEquals( statefulRegionDef1, pipeline1.getRegionDef() );
        assertEquals( 0, pipeline1.getOperatorIndex( statefulRegionDef1.getOperators().get( 0 ) ) );
        final DownstreamCollector pipeline1Downstream = pipeline1.getDownstreamCollector( 0 );
        assertTrue( pipeline1Downstream instanceof IngestionTimeInjector );
        assertTrue( ( (IngestionTimeInjector) pipeline1Downstream ).getDownstream() instanceof DownstreamCollector1 );
        assertEquals( ( (Supplier<OperatorQueue>) ( ( (IngestionTimeInjector) pipeline1Downstream ).getDownstream() ) ).get(),
                      pipeline3.getPipelineReplica( 0 ).getEffectiveQueue() );

        assertEquals( statefulRegionDef2, pipeline2.getRegionDef() );
        assertEquals( 0, pipeline2.getOperatorIndex( statefulRegionDef2.getOperators().get( 0 ) ) );
        final DownstreamCollector pipeline2Downstream = pipeline2.getDownstreamCollector( 0 );
        assertTrue( pipeline2Downstream instanceof IngestionTimeInjector );
        assertTrue( ( (IngestionTimeInjector) pipeline2Downstream ).getDownstream() instanceof DownstreamCollector1 );
        assertEquals( ( (Supplier<OperatorQueue>) ( ( (IngestionTimeInjector) pipeline2Downstream ).getDownstream() ) ).get(),
                      pipeline3.getPipelineReplica( 0 ).getEffectiveQueue() );

        assertEquals( statelessRegionDef, pipeline3.getRegionDef() );
        assertEquals( 0, pipeline3.getOperatorIndex( operatorDef3 ) );
        assertTrue( pipeline3.getDownstreamCollector( 0 ) instanceof LatencyRecorder );
    }

    @Test
    public void test_singleRegion_connectedToTwoRegions ()
    {
        final OperatorDef operatorDef1 = OperatorDefBuilder.newInstance( "op1", StatefulOperatorInput0Output1.class ).build();
        final OperatorDef operatorDef2 = OperatorDefBuilder.newInstance( "op2", StatelessOperatorInput1Output1.class ).build();
        final OperatorDef operatorDef3 = OperatorDefBuilder.newInstance( "op3", PartitionedStatefulOperatorInput2Output2.class )
                                                           .setPartitionFieldNames( singletonList( "field1" ) )
                                                           .build();

        final FlowDef flow = new FlowDefBuilder().add( operatorDef1 )
                                                 .add( operatorDef2 )
                                                 .add( operatorDef3 )
                                                 .connect( "op1", "op2" )
                                                 .connect( "op1", 0, "op3", 0 )
                                                 .connect( "op1", 0, "op3", 1 )
                                                 .build();

        final List<RegionDef> regions = regionDefFormer.createRegions( flow );
        assertEquals( 3, regions.size() );

        final RegionDef statefulRegionDef = findRegion( regions, operatorDef1 );
        final RegionDef statelessRegionDef = findRegion( regions, operatorDef2 );
        final RegionDef partitionedStatefulRegionDef = findRegion( regions, operatorDef3 );

        final RegionExecPlan regionExecPlan1 = new RegionExecPlan( statefulRegionDef, singletonList( 0 ), 1 );
        final RegionExecPlan regionExecPlan2 = new RegionExecPlan( statelessRegionDef, singletonList( 0 ), 1 );
        final RegionExecPlan regionExecPlan3 = new RegionExecPlan( partitionedStatefulRegionDef, singletonList( 0 ), 1 );

        final List<Pipeline> pipelines = pipelineManager.createPipelines( flow,
                                                                          asList( regionExecPlan1, regionExecPlan2, regionExecPlan3 ) );

        final Pipeline pipeline1 = pipelines.get( 0 );
        final Pipeline pipeline2 = pipelines.get( 1 );
        final Pipeline pipeline3 = pipelines.get( 2 );

        assertEquals( INITIAL, pipeline1.getPipelineStatus() );
        assertEquals( INITIAL, pipeline2.getPipelineStatus() );
        assertEquals( INITIAL, pipeline3.getPipelineStatus() );

        assertEquals( statefulRegionDef, pipeline1.getRegionDef() );
        assertEquals( 0, pipeline1.getOperatorIndex( statefulRegionDef.getOperators().get( 0 ) ) );
        final DownstreamCollector sourceDownstream = pipeline1.getDownstreamCollector( 0 );
        assertTrue( sourceDownstream instanceof IngestionTimeInjector );
        assertTrue( ( (IngestionTimeInjector) sourceDownstream ).getDownstream() instanceof CompositeDownstreamCollector );

        assertEquals( statelessRegionDef, pipeline2.getRegionDef() );
        assertEquals( 0, pipeline2.getOperatorIndex( statelessRegionDef.getOperators().get( 0 ) ) );
        assertTrue( pipeline2.getDownstreamCollector( 0 ) instanceof LatencyRecorder );

        assertEquals( partitionedStatefulRegionDef, pipeline3.getRegionDef() );
        assertEquals( 0, pipeline3.getOperatorIndex( operatorDef3 ) );
        assertTrue( pipeline3.getDownstreamCollector( 0 ) instanceof LatencyRecorder );
    }

    private RegionDef findRegion ( Collection<RegionDef> regions, final OperatorDef operator )
    {
        return regions.stream().filter( regionDef -> regionDef.indexOf( operator ) != -1 ).findFirst().get();
    }

    @OperatorSpec( type = STATEFUL, inputPortCount = 0, outputPortCount = 1 )
    @OperatorSchema( outputs = { @PortSchema( portIndex = 0, scope = EXACT_FIELD_SET, fields = { @SchemaField( name = "field1", type = Integer.class ) } ) } )
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


    @OperatorSpec( type = STATEFUL, inputPortCount = 1, outputPortCount = 1 )
    @OperatorSchema( inputs = { @PortSchema( portIndex = 0, scope = EXACT_FIELD_SET, fields = { @SchemaField( name = "field1", type = Integer.class ) } ) }, outputs = {
            @PortSchema( portIndex = 0, scope = EXACT_FIELD_SET, fields = { @SchemaField( name = "field1", type = Integer.class ) } ) } )
    public static class StatefulOperatorInput1Output1 implements Operator
    {

        @Override
        public SchedulingStrategy init ( final InitCtx ctx )
        {
            return scheduleWhenTuplesAvailableOnDefaultPort( 1 );
        }

        @Override
        public void invoke ( final InvocationCtx ctx )
        {

        }

    }


    @OperatorSpec( type = STATELESS, inputPortCount = 1, outputPortCount = 1 )
    @OperatorSchema( inputs = { @PortSchema( portIndex = 0, scope = EXACT_FIELD_SET, fields = { @SchemaField( name = "field1", type = Integer.class ) } ) }, outputs = {
            @PortSchema( portIndex = 0, scope = EXACT_FIELD_SET, fields = { @SchemaField( name = "field1", type = Integer.class ) } ) } )
    public static class StatelessOperatorInput1Output1 implements Operator
    {

        @Override
        public SchedulingStrategy init ( final InitCtx ctx )
        {
            return scheduleWhenTuplesAvailableOnDefaultPort( 1 );
        }

        @Override
        public void invoke ( final InvocationCtx ctx )
        {

        }

    }


    @OperatorSpec( type = PARTITIONED_STATEFUL, inputPortCount = 2, outputPortCount = 2 )
    @OperatorSchema( inputs = { @PortSchema( portIndex = 0, scope = EXACT_FIELD_SET, fields = { @SchemaField( name = "field1", type = Integer.class ) } ),
                                @PortSchema( portIndex = 1, scope = EXACT_FIELD_SET, fields = { @SchemaField( name = "field1", type = Integer.class ) } ) }, outputs = {
            @PortSchema( portIndex = 0, scope = EXACT_FIELD_SET, fields = { @SchemaField( name = "field1", type = Integer.class ) } ) } )
    public static class PartitionedStatefulOperatorInput2Output2 implements Operator
    {

        @Override
        public SchedulingStrategy init ( final InitCtx ctx )
        {
            final int[] openPorts = IntStream.range( 0, ctx.getInputPortCount() ).filter( ctx::isInputPortOpen ).toArray();
            return scheduleWhenTuplesAvailableOnAny( AT_LEAST, ctx.getInputPortCount(), 1, openPorts );
        }

        @Override
        public void invoke ( final InvocationCtx ctx )
        {

        }

    }

}
