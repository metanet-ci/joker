package cs.bilkent.joker.engine.pipeline.impl;

import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

import org.junit.Before;
import org.junit.Test;
import com.google.inject.Guice;
import com.google.inject.Injector;

import cs.bilkent.joker.JokerModule;
import cs.bilkent.joker.engine.config.JokerConfig;
import cs.bilkent.joker.engine.pipeline.DownstreamTupleSender;
import static cs.bilkent.joker.engine.pipeline.OperatorReplicaStatus.INITIAL;
import cs.bilkent.joker.engine.pipeline.Pipeline;
import cs.bilkent.joker.engine.pipeline.PipelineManager;
import cs.bilkent.joker.engine.pipeline.UpstreamConnectionStatus;
import static cs.bilkent.joker.engine.pipeline.UpstreamConnectionStatus.ACTIVE;
import static cs.bilkent.joker.engine.pipeline.UpstreamConnectionStatus.CLOSED;
import cs.bilkent.joker.engine.pipeline.UpstreamContext;
import cs.bilkent.joker.engine.pipeline.impl.PipelineManagerImpl.NopDownstreamTupleSender;
import cs.bilkent.joker.engine.pipeline.impl.downstreamtuplesender.CompositeDownstreamTupleSender;
import cs.bilkent.joker.engine.pipeline.impl.downstreamtuplesender.DownstreamTupleSender1;
import cs.bilkent.joker.engine.pipeline.impl.downstreamtuplesender.PartitionedDownstreamTupleSender1;
import cs.bilkent.joker.engine.region.RegionConfig;
import cs.bilkent.joker.engine.region.RegionDef;
import cs.bilkent.joker.engine.region.RegionDefFormer;
import cs.bilkent.joker.engine.supervisor.Supervisor;
import cs.bilkent.joker.engine.tuplequeue.OperatorTupleQueue;
import cs.bilkent.joker.flow.FlowDef;
import cs.bilkent.joker.flow.FlowDefBuilder;
import cs.bilkent.joker.operator.InitializationContext;
import cs.bilkent.joker.operator.InvocationContext;
import cs.bilkent.joker.operator.Operator;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.OperatorDefBuilder;
import cs.bilkent.joker.operator.scheduling.ScheduleWhenAvailable;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.scheduleWhenTuplesAvailableOnAny;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.scheduleWhenTuplesAvailableOnDefaultPort;
import cs.bilkent.joker.operator.scheduling.SchedulingStrategy;
import cs.bilkent.joker.operator.schema.annotation.OperatorSchema;
import cs.bilkent.joker.operator.schema.annotation.PortSchema;
import static cs.bilkent.joker.operator.schema.annotation.PortSchemaScope.EXACT_FIELD_SET;
import cs.bilkent.joker.operator.schema.annotation.SchemaField;
import cs.bilkent.joker.operator.spec.OperatorSpec;
import cs.bilkent.joker.operator.spec.OperatorType;
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

    private Supervisor supervisor;

    @Before
    public void init ()
    {
        final Injector injector = Guice.createInjector( new JokerModule( jokerConfig ) );
        regionDefFormer = injector.getInstance( RegionDefFormer.class );
        pipelineManager = (PipelineManagerImpl) injector.getInstance( PipelineManager.class );
        supervisor = injector.getInstance( Supervisor.class );
    }

    @Test
    public void test_singleStatefulRegion ()
    {
        final OperatorDef operatorDef = OperatorDefBuilder.newInstance( "op1", StatefulOperatorInput0Output1.class ).build();
        final FlowDef flow = new FlowDefBuilder().add( operatorDef ).build();

        final List<RegionDef> regions = regionDefFormer.createRegions( flow );
        final RegionConfig regionConfig = new RegionConfig( regions.get( 0 ), singletonList( 0 ), 1 );

        final List<Pipeline> pipelines = pipelineManager.createPipelines( flow, singletonList( regionConfig ) );

        assertEquals( 1, pipelines.size() );

        final Pipeline pipeline = pipelines.get( 0 );
        assertEquals( 0, pipeline.getOperatorIndex( operatorDef ) );
        assertEquals( INITIAL, pipeline.getPipelineStatus() );

        assertNotNull( pipeline.getPipelineReplica( 0 ) );
        assertEquals( new UpstreamContext( 0, new UpstreamConnectionStatus[] {} ), pipeline.getUpstreamContext() );
        assertNotNull( pipeline.getDownstreamTupleSender( 0 ) );
    }

    @Test
    public void test_partitionedStatefulRegion_statefulRegion ()
    {
        final OperatorDef operatorDef1 = OperatorDefBuilder.newInstance( "op1", PartitionedStatefulOperatorInput2Output2.class )
                                                           .setPartitionFieldNames( singletonList( "field1" ) )
                                                           .build();
        final OperatorDef operatorDef2 = OperatorDefBuilder.newInstance( "op2", StatefulOperatorInput1Output1.class ).build();
        final FlowDef flow = new FlowDefBuilder().add( operatorDef1 ).add( operatorDef2 ).connect( "op1", "op2" ).build();
        final List<RegionDef> regions = regionDefFormer.createRegions( flow );
        assertEquals( 2, regions.size() );
        final RegionDef partitionedStatefulRegionDef = findRegion( regions, PARTITIONED_STATEFUL );
        final RegionDef statefulRegionDef = findRegion( regions, STATEFUL );
        final RegionConfig regionConfig1 = new RegionConfig( partitionedStatefulRegionDef, singletonList( 0 ), 2 );
        final RegionConfig regionConfig2 = new RegionConfig( statefulRegionDef, singletonList( 0 ), 1 );
        final List<Pipeline> pipelines = pipelineManager.createPipelines( flow, asList( regionConfig1, regionConfig2 ) );

        assertEquals( 2, pipelines.size() );

        final Pipeline pipeline1 = pipelines.get( 0 );
        assertEquals( new UpstreamContext( 0, new UpstreamConnectionStatus[] { CLOSED, CLOSED } ), pipeline1.getUpstreamContext() );
        assertEquals( partitionedStatefulRegionDef, pipeline1.getRegionDef() );
        assertEquals( 0, pipeline1.getOperatorIndex( operatorDef1 ) );
        assertNotEquals( pipeline1.getPipelineReplica( 0 ), pipeline1.getPipelineReplica( 1 ) );
        assertTrue( pipeline1.getDownstreamTupleSender( 0 ) instanceof DownstreamTupleSender1 );
        assertTrue( pipeline1.getDownstreamTupleSender( 1 ) instanceof DownstreamTupleSender1 );
        assertEquals( INITIAL, pipeline1.getPipelineStatus() );

        final Pipeline pipeline2 = pipelines.get( 1 );
        assertEquals( INITIAL, pipeline2.getPipelineStatus() );
        assertEquals( ( (Supplier<OperatorTupleQueue>) pipeline1.getDownstreamTupleSender( 0 ) ).get(),
                      pipeline2.getPipelineReplica( 0 ).getPipelineTupleQueue() );
        assertEquals( ( (Supplier<OperatorTupleQueue>) pipeline1.getDownstreamTupleSender( 1 ) ).get(),
                      pipeline2.getPipelineReplica( 0 ).getPipelineTupleQueue() );
    }

    @Test
    public void test_partitionedStatefulRegion_statefulRegionAndStatelessRegion ()
    {
        final OperatorDef operatorDef1 = OperatorDefBuilder.newInstance( "op1", PartitionedStatefulOperatorInput2Output2.class )
                                                           .setPartitionFieldNames( singletonList( "field1" ) )
                                                           .build();
        final OperatorDef operatorDef2 = OperatorDefBuilder.newInstance( "op2", StatefulOperatorInput1Output1.class ).build();
        final OperatorDef operatorDef3 = OperatorDefBuilder.newInstance( "op3", StatelessOperatorInput1Output1.class ).build();
        final FlowDef flow = new FlowDefBuilder().add( operatorDef1 )
                                                 .add( operatorDef2 )
                                                 .add( operatorDef3 )
                                                 .connect( "op1", "op2" )
                                                 .connect( "op1", "op3" )
                                                 .build();
        final List<RegionDef> regions = regionDefFormer.createRegions( flow );
        assertEquals( 3, regions.size() );

        final RegionDef partitionedStatefulRegionDef = findRegion( regions, PARTITIONED_STATEFUL );
        final RegionDef statefulRegionDef = findRegion( regions, STATEFUL );
        final RegionDef statelessRegionDef = findRegion( regions, STATELESS );
        final RegionConfig regionConfig1 = new RegionConfig( partitionedStatefulRegionDef, singletonList( 0 ), 2 );
        final RegionConfig regionConfig2 = new RegionConfig( statefulRegionDef, singletonList( 0 ), 1 );
        final RegionConfig regionConfig3 = new RegionConfig( statelessRegionDef, singletonList( 0 ), 2 );
        final List<Pipeline> pipelines = pipelineManager.createPipelines( flow, asList( regionConfig1, regionConfig2, regionConfig3 ) );

        assertEquals( 3, pipelines.size() );

        final Pipeline pipeline1 = pipelines.get( 0 );
        final Pipeline pipeline2 = pipelines.get( 1 );
        final Pipeline pipeline3 = pipelines.get( 2 );

        assertEquals( INITIAL, pipeline1.getPipelineStatus() );
        assertEquals( INITIAL, pipeline2.getPipelineStatus() );
        assertEquals( INITIAL, pipeline3.getPipelineStatus() );

        assertEquals( new UpstreamContext( 0, new UpstreamConnectionStatus[] { CLOSED, CLOSED } ), pipeline1.getUpstreamContext() );
        assertEquals( partitionedStatefulRegionDef, pipeline1.getRegionDef() );
        assertEquals( 0, pipeline1.getOperatorIndex( operatorDef1 ) );
        assertNotEquals( pipeline1.getPipelineReplica( 0 ), pipeline1.getPipelineReplica( 1 ) );

        assertTrue( pipeline1.getDownstreamTupleSender( 0 ) instanceof CompositeDownstreamTupleSender );
        assertTrue( pipeline1.getDownstreamTupleSender( 1 ) instanceof CompositeDownstreamTupleSender );
        final DownstreamTupleSender[] senders0 = ( (CompositeDownstreamTupleSender) pipeline1.getDownstreamTupleSender( 0 ) )
                                                         .getDownstreamTupleSenders();

        final DownstreamTupleSender[] senders1 = ( (CompositeDownstreamTupleSender) pipeline1.getDownstreamTupleSender( 1 ) )
                                                         .getDownstreamTupleSenders();

        if ( ( (Supplier<OperatorTupleQueue>) senders0[ 0 ] ).get().equals( pipeline2.getPipelineReplica( 0 ).getPipelineTupleQueue() ) )
        {
            assertEquals( ( (Supplier<OperatorTupleQueue>) senders0[ 1 ] ).get(),
                          pipeline3.getPipelineReplica( 0 ).getPipelineTupleQueue() );
        }
        else if ( ( (Supplier<OperatorTupleQueue>) senders0[ 1 ] ).get()
                                                                  .equals( pipeline2.getPipelineReplica( 0 ).getPipelineTupleQueue() ) )
        {
            assertEquals( ( (Supplier<OperatorTupleQueue>) senders0[ 0 ] ).get(),
                          pipeline3.getPipelineReplica( 0 ).getPipelineTupleQueue() );
        }
        else
        {
            fail();
        }

        if ( ( (Supplier<OperatorTupleQueue>) senders1[ 0 ] ).get().equals( pipeline2.getPipelineReplica( 0 ).getPipelineTupleQueue() ) )
        {
            assertEquals( ( (Supplier<OperatorTupleQueue>) senders1[ 1 ] ).get(),
                          pipeline3.getPipelineReplica( 1 ).getPipelineTupleQueue() );
        }
        else if ( ( (Supplier<OperatorTupleQueue>) senders1[ 1 ] ).get()
                                                                  .equals( pipeline2.getPipelineReplica( 0 ).getPipelineTupleQueue() ) )
        {
            assertEquals( ( (Supplier<OperatorTupleQueue>) senders1[ 0 ] ).get(),
                          pipeline3.getPipelineReplica( 1 ).getPipelineTupleQueue() );
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
        final RegionDef statefulRegionDef = findRegion( regions, STATEFUL );
        final RegionDef partitionedStatefulRegionDef = findRegion( regions, PARTITIONED_STATEFUL );
        final RegionConfig regionConfig1 = new RegionConfig( statefulRegionDef, singletonList( 0 ), 1 );
        final RegionConfig regionConfig2 = new RegionConfig( partitionedStatefulRegionDef, asList( 0, 1 ), 2 );

        final List<Pipeline> pipelines = pipelineManager.createPipelines( flow, asList( regionConfig1, regionConfig2 ) );

        assertEquals( 3, pipelines.size() );

        final Pipeline pipeline1 = pipelines.get( 0 );
        final Pipeline pipeline2 = pipelines.get( 1 );
        final Pipeline pipeline3 = pipelines.get( 2 );

        assertEquals( INITIAL, pipeline1.getPipelineStatus() );
        assertEquals( INITIAL, pipeline2.getPipelineStatus() );
        assertEquals( INITIAL, pipeline3.getPipelineStatus() );

        assertEquals( statefulRegionDef, pipeline1.getRegionDef() );
        assertEquals( 0, pipeline1.getOperatorIndex( operatorDef1 ) );
        assertTrue( pipeline1.getDownstreamTupleSender( 0 ) instanceof PartitionedDownstreamTupleSender1 );
        assertArrayEquals( ( (Supplier<OperatorTupleQueue[]>) pipeline1.getDownstreamTupleSender( 0 ) ).get(),
                           new OperatorTupleQueue[] { pipeline2.getPipelineReplica( 0 ).getPipelineTupleQueue(),
                                                      pipeline2.getPipelineReplica( 1 ).getPipelineTupleQueue() } );

        assertEquals( new UpstreamContext( 0, new UpstreamConnectionStatus[] { ACTIVE, CLOSED } ), pipeline2.getUpstreamContext() );
        assertEquals( partitionedStatefulRegionDef, pipeline2.getRegionDef() );
        assertEquals( 0, pipeline2.getOperatorIndex( operatorDef2 ) );
        assertFalse( pipeline2.getPipelineReplica( 0 ) == pipeline2.getPipelineReplica( 1 ) );
        assertTrue( pipeline2.getDownstreamTupleSender( 0 ) instanceof DownstreamTupleSender1 );
        assertTrue( pipeline2.getDownstreamTupleSender( 1 ) instanceof DownstreamTupleSender1 );
        assertEquals( ( (Supplier<OperatorTupleQueue>) pipeline2.getDownstreamTupleSender( 0 ) ).get(),
                      pipeline3.getPipelineReplica( 0 ).getPipelineTupleQueue() );
        assertEquals( ( (Supplier<OperatorTupleQueue>) pipeline2.getDownstreamTupleSender( 1 ) ).get(),
                      pipeline3.getPipelineReplica( 1 ).getPipelineTupleQueue() );

        assertEquals( new UpstreamContext( 0, new UpstreamConnectionStatus[] { ACTIVE } ), pipeline3.getUpstreamContext() );
        assertEquals( partitionedStatefulRegionDef, pipeline3.getRegionDef() );
        assertEquals( 0, pipeline3.getOperatorIndex( operatorDef3 ) );
        assertFalse( pipeline3.getPipelineReplica( 0 ) == pipeline3.getPipelineReplica( 1 ) );
        assertTrue( pipeline3.getDownstreamTupleSender( 0 ) instanceof NopDownstreamTupleSender );
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

        final RegionDef statefulRegionDef = findRegion( regions, STATEFUL );
        final RegionDef partitionedStatefulRegionDef = findRegion( regions, PARTITIONED_STATEFUL );
        final RegionConfig regionConfig1 = new RegionConfig( statefulRegionDef, singletonList( 0 ), 1 );
        final RegionConfig regionConfig2 = new RegionConfig( partitionedStatefulRegionDef, asList( 0, 1 ), 2 );

        final List<Pipeline> pipelines = pipelineManager.createPipelines( flow, asList( regionConfig1, regionConfig2 ) );

        assertEquals( 3, pipelines.size() );

        final Pipeline pipeline1 = pipelines.get( 0 );
        final Pipeline pipeline2 = pipelines.get( 1 );
        final Pipeline pipeline3 = pipelines.get( 2 );

        assertEquals( INITIAL, pipeline1.getPipelineStatus() );
        assertEquals( INITIAL, pipeline2.getPipelineStatus() );
        assertEquals( INITIAL, pipeline3.getPipelineStatus() );

        assertEquals( statefulRegionDef, pipeline1.getRegionDef() );
        assertEquals( 0, pipeline1.getOperatorIndex( operatorDef1 ) );
        assertTrue( pipeline1.getDownstreamTupleSender( 0 ) instanceof PartitionedDownstreamTupleSender1 );
        assertArrayEquals( ( (Supplier<OperatorTupleQueue[]>) pipeline1.getDownstreamTupleSender( 0 ) ).get(),
                           new OperatorTupleQueue[] { pipeline2.getPipelineReplica( 0 ).getPipelineTupleQueue(),
                                                      pipeline2.getPipelineReplica( 1 ).getPipelineTupleQueue() } );

        assertEquals( new UpstreamContext( 0, new UpstreamConnectionStatus[] { ACTIVE } ), pipeline2.getUpstreamContext() );
        assertEquals( partitionedStatefulRegionDef, pipeline2.getRegionDef() );
        assertEquals( 0, pipeline2.getOperatorIndex( operatorDef2 ) );
        assertFalse( pipeline2.getPipelineReplica( 0 ) == pipeline2.getPipelineReplica( 1 ) );
        assertTrue( pipeline2.getDownstreamTupleSender( 0 ) instanceof DownstreamTupleSender1 );
        assertTrue( pipeline2.getDownstreamTupleSender( 1 ) instanceof DownstreamTupleSender1 );
        assertEquals( ( (Supplier<OperatorTupleQueue>) pipeline2.getDownstreamTupleSender( 0 ) ).get(),
                      pipeline3.getPipelineReplica( 0 ).getPipelineTupleQueue() );
        assertEquals( ( (Supplier<OperatorTupleQueue>) pipeline2.getDownstreamTupleSender( 1 ) ).get(),
                      pipeline3.getPipelineReplica( 1 ).getPipelineTupleQueue() );

        assertEquals( new UpstreamContext( 0, new UpstreamConnectionStatus[] { ACTIVE, CLOSED } ), pipeline3.getUpstreamContext() );
        assertEquals( partitionedStatefulRegionDef, pipeline3.getRegionDef() );
        assertEquals( 0, pipeline3.getOperatorIndex( operatorDef3 ) );
        assertFalse( pipeline3.getPipelineReplica( 0 ) == pipeline3.getPipelineReplica( 1 ) );
        assertTrue( pipeline3.getDownstreamTupleSender( 0 ) instanceof NopDownstreamTupleSender );
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

        final RegionDef statelessRegionDef = findRegion( regions, STATELESS );

        final List<RegionDef> statefulRegionDefs = regions.stream()
                                                          .filter( regionDef -> regionDef.getRegionType() == STATEFUL )
                                                          .collect( toList() );

        final RegionDef statefulRegionDef1 = statefulRegionDefs.get( 0 ), statefulRegionDef2 = statefulRegionDefs.get( 1 );

        final RegionConfig regionConfig1 = new RegionConfig( statefulRegionDef1, singletonList( 0 ), 1 );
        final RegionConfig regionConfig2 = new RegionConfig( statefulRegionDef2, singletonList( 0 ), 1 );
        final RegionConfig regionConfig3 = new RegionConfig( statelessRegionDef, singletonList( 0 ), 1 );

        final List<Pipeline> pipelines = pipelineManager.createPipelines( flow, asList( regionConfig1, regionConfig2, regionConfig3 ) );

        final Pipeline pipeline1 = pipelines.get( 0 );
        final Pipeline pipeline2 = pipelines.get( 1 );
        final Pipeline pipeline3 = pipelines.get( 2 );

        assertEquals( INITIAL, pipeline1.getPipelineStatus() );
        assertEquals( INITIAL, pipeline2.getPipelineStatus() );
        assertEquals( INITIAL, pipeline3.getPipelineStatus() );

        assertEquals( statefulRegionDef1, pipeline1.getRegionDef() );
        assertEquals( 0, pipeline1.getOperatorIndex( statefulRegionDef1.getOperators().get( 0 ) ) );
        assertTrue( pipeline1.getDownstreamTupleSender( 0 ) instanceof DownstreamTupleSender1 );
        assertEquals( ( (Supplier<OperatorTupleQueue>) pipeline1.getDownstreamTupleSender( 0 ) ).get(),
                      pipeline3.getPipelineReplica( 0 ).getPipelineTupleQueue() );

        assertEquals( statefulRegionDef2, pipeline2.getRegionDef() );
        assertEquals( 0, pipeline2.getOperatorIndex( statefulRegionDef2.getOperators().get( 0 ) ) );
        assertTrue( pipeline2.getDownstreamTupleSender( 0 ) instanceof DownstreamTupleSender1 );
        assertEquals( ( (Supplier<OperatorTupleQueue>) pipeline2.getDownstreamTupleSender( 0 ) ).get(),
                      pipeline3.getPipelineReplica( 0 ).getPipelineTupleQueue() );

        assertEquals( new UpstreamContext( 0, new UpstreamConnectionStatus[] { ACTIVE } ), pipeline3.getUpstreamContext() );
        assertEquals( statelessRegionDef, pipeline3.getRegionDef() );
        assertEquals( 0, pipeline3.getOperatorIndex( operatorDef3 ) );
        assertTrue( pipeline3.getDownstreamTupleSender( 0 ) instanceof NopDownstreamTupleSender );
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

        final RegionDef statefulRegionDef = findRegion( regions, STATEFUL );
        final RegionDef statelessRegionDef = findRegion( regions, STATELESS );
        final RegionDef partitionedStatefulRegionDef = findRegion( regions, PARTITIONED_STATEFUL );

        final RegionConfig regionConfig1 = new RegionConfig( statefulRegionDef, singletonList( 0 ), 1 );
        final RegionConfig regionConfig2 = new RegionConfig( statelessRegionDef, singletonList( 0 ), 1 );
        final RegionConfig regionConfig3 = new RegionConfig( partitionedStatefulRegionDef, singletonList( 0 ), 1 );

        final List<Pipeline> pipelines = pipelineManager.createPipelines( flow, asList( regionConfig1, regionConfig2, regionConfig3 ) );

        final Pipeline pipeline1 = pipelines.get( 0 );
        final Pipeline pipeline2 = pipelines.get( 1 );
        final Pipeline pipeline3 = pipelines.get( 2 );

        assertEquals( INITIAL, pipeline1.getPipelineStatus() );
        assertEquals( INITIAL, pipeline2.getPipelineStatus() );
        assertEquals( INITIAL, pipeline3.getPipelineStatus() );

        assertEquals( statefulRegionDef, pipeline1.getRegionDef() );
        assertEquals( 0, pipeline1.getOperatorIndex( statefulRegionDef.getOperators().get( 0 ) ) );
        assertTrue( pipeline1.getDownstreamTupleSender( 0 ) instanceof CompositeDownstreamTupleSender );

        assertEquals( statelessRegionDef, pipeline2.getRegionDef() );
        assertEquals( new UpstreamContext( 0, new UpstreamConnectionStatus[] { ACTIVE } ), pipeline2.getUpstreamContext() );
        assertEquals( 0, pipeline2.getOperatorIndex( statelessRegionDef.getOperators().get( 0 ) ) );
        assertTrue( pipeline2.getDownstreamTupleSender( 0 ) instanceof NopDownstreamTupleSender );

        assertEquals( new UpstreamContext( 0, new UpstreamConnectionStatus[] { ACTIVE, ACTIVE } ), pipeline3.getUpstreamContext() );
        assertEquals( partitionedStatefulRegionDef, pipeline3.getRegionDef() );
        assertEquals( 0, pipeline3.getOperatorIndex( operatorDef3 ) );
        assertTrue( pipeline3.getDownstreamTupleSender( 0 ) instanceof NopDownstreamTupleSender );
    }

    private RegionDef findRegion ( Collection<RegionDef> regions, final OperatorType regionType )
    {
        return regions.stream().filter( regionDef -> regionDef.getRegionType() == regionType ).findFirst().get();
    }

    @OperatorSpec( type = STATEFUL, inputPortCount = 0, outputPortCount = 1 )
    @OperatorSchema( outputs = { @PortSchema( portIndex = 0, scope = EXACT_FIELD_SET, fields = { @SchemaField( name = "field1", type = Integer.class ) } ) } )
    public static class StatefulOperatorInput0Output1 implements Operator
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


    @OperatorSpec( type = STATEFUL, inputPortCount = 1, outputPortCount = 1 )
    @OperatorSchema( inputs = { @PortSchema( portIndex = 0, scope = EXACT_FIELD_SET, fields = { @SchemaField( name = "field1", type = Integer.class ) } ) }, outputs = { @PortSchema( portIndex = 0, scope = EXACT_FIELD_SET, fields = { @SchemaField( name = "field1", type = Integer.class ) } ) } )
    public static class StatefulOperatorInput1Output1 implements Operator
    {

        @Override
        public SchedulingStrategy init ( final InitializationContext context )
        {
            return scheduleWhenTuplesAvailableOnDefaultPort( 1 );
        }

        @Override
        public void invoke ( final InvocationContext invocationContext )
        {

        }

    }


    @OperatorSpec( type = STATELESS, inputPortCount = 1, outputPortCount = 1 )
    @OperatorSchema( inputs = { @PortSchema( portIndex = 0, scope = EXACT_FIELD_SET, fields = { @SchemaField( name = "field1", type = Integer.class ) } ) }, outputs = { @PortSchema( portIndex = 0, scope = EXACT_FIELD_SET, fields = { @SchemaField( name = "field1", type = Integer.class ) } ) } )
    private static class StatelessOperatorInput1Output1 implements Operator
    {

        @Override
        public SchedulingStrategy init ( final InitializationContext context )
        {
            return null;
        }

        @Override
        public void invoke ( final InvocationContext invocationContext )
        {

        }

    }


    @OperatorSpec( type = PARTITIONED_STATEFUL, inputPortCount = 2, outputPortCount = 2 )
    @OperatorSchema( inputs = { @PortSchema( portIndex = 0, scope = EXACT_FIELD_SET, fields = { @SchemaField( name = "field1", type = Integer.class ) } ),
                                @PortSchema( portIndex = 1, scope = EXACT_FIELD_SET, fields = { @SchemaField( name = "field1", type = Integer.class ) } ) }, outputs = { @PortSchema( portIndex = 0, scope = EXACT_FIELD_SET, fields = { @SchemaField( name = "field1", type = Integer.class ) } ) } )
    public static class PartitionedStatefulOperatorInput2Output2 implements Operator
    {

        @Override
        public SchedulingStrategy init ( final InitializationContext context )
        {
            return scheduleWhenTuplesAvailableOnAny( 2, 1, 0, 1 );
        }

        @Override
        public void invoke ( final InvocationContext invocationContext )
        {

        }

    }

}
