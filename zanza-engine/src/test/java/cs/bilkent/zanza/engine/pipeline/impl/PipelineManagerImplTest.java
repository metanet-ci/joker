package cs.bilkent.zanza.engine.pipeline.impl;

import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

import org.junit.Before;
import org.junit.Test;
import com.google.inject.Guice;
import com.google.inject.Injector;

import cs.bilkent.zanza.ZanzaModule;
import cs.bilkent.zanza.engine.config.ZanzaConfig;
import cs.bilkent.zanza.engine.pipeline.DownstreamTupleSender;
import static cs.bilkent.zanza.engine.pipeline.OperatorReplicaStatus.RUNNING;
import cs.bilkent.zanza.engine.pipeline.Pipeline;
import cs.bilkent.zanza.engine.pipeline.PipelineManager;
import cs.bilkent.zanza.engine.pipeline.UpstreamConnectionStatus;
import static cs.bilkent.zanza.engine.pipeline.UpstreamConnectionStatus.ACTIVE;
import static cs.bilkent.zanza.engine.pipeline.UpstreamConnectionStatus.NO_CONNECTION;
import cs.bilkent.zanza.engine.pipeline.UpstreamContext;
import cs.bilkent.zanza.engine.pipeline.impl.PipelineManagerImpl.NopDownstreamTupleSender;
import cs.bilkent.zanza.engine.pipeline.impl.downstreamtuplesender.CompositeDownstreamTupleSender;
import cs.bilkent.zanza.engine.pipeline.impl.downstreamtuplesender.DownstreamTupleSender1;
import cs.bilkent.zanza.engine.pipeline.impl.downstreamtuplesender.PartitionedDownstreamTupleSender1;
import cs.bilkent.zanza.engine.region.RegionConfig;
import cs.bilkent.zanza.engine.region.RegionDef;
import cs.bilkent.zanza.engine.region.RegionDefFormer;
import cs.bilkent.zanza.engine.supervisor.Supervisor;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueContext;
import cs.bilkent.zanza.flow.FlowDef;
import cs.bilkent.zanza.flow.FlowDefBuilder;
import cs.bilkent.zanza.flow.OperatorDef;
import cs.bilkent.zanza.flow.OperatorDefBuilder;
import cs.bilkent.zanza.operator.InitializationContext;
import cs.bilkent.zanza.operator.InvocationContext;
import cs.bilkent.zanza.operator.Operator;
import cs.bilkent.zanza.operator.scheduling.ScheduleWhenAvailable;
import static cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.scheduleWhenTuplesAvailableOnAny;
import static cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.scheduleWhenTuplesAvailableOnDefaultPort;
import cs.bilkent.zanza.operator.scheduling.SchedulingStrategy;
import cs.bilkent.zanza.operator.schema.annotation.OperatorSchema;
import cs.bilkent.zanza.operator.schema.annotation.PortSchema;
import static cs.bilkent.zanza.operator.schema.annotation.PortSchemaScope.EXACT_FIELD_SET;
import cs.bilkent.zanza.operator.schema.annotation.SchemaField;
import cs.bilkent.zanza.operator.spec.OperatorSpec;
import cs.bilkent.zanza.operator.spec.OperatorType;
import static cs.bilkent.zanza.operator.spec.OperatorType.PARTITIONED_STATEFUL;
import static cs.bilkent.zanza.operator.spec.OperatorType.STATEFUL;
import static cs.bilkent.zanza.operator.spec.OperatorType.STATELESS;
import cs.bilkent.zanza.testutils.ZanzaAbstractTest;
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

public class PipelineManagerImplTest extends ZanzaAbstractTest
{

    private final ZanzaConfig zanzaConfig = new ZanzaConfig();

    private RegionDefFormer regionDefFormer;

    private PipelineManager pipelineManager;

    private Supervisor supervisor;

    @Before
    public void init ()
    {
        final Injector injector = Guice.createInjector( new ZanzaModule( zanzaConfig ) );
        regionDefFormer = injector.getInstance( RegionDefFormer.class );
        pipelineManager = injector.getInstance( PipelineManager.class );
        supervisor = injector.getInstance( Supervisor.class );
    }

    @Test
    public void test_singleStatefulRegion ()
    {
        final OperatorDef operatorDef = OperatorDefBuilder.newInstance( "op1", StatefulOperatorInput0Output1.class ).build();
        final FlowDef flow = new FlowDefBuilder().add( operatorDef ).build();

        final List<RegionDef> regions = regionDefFormer.createRegions( flow );
        final RegionConfig regionConfig = new RegionConfig( regions.get( 0 ), singletonList( 0 ), 1 );

        final List<Pipeline> pipelines = pipelineManager.createPipelines( supervisor, flow, singletonList( regionConfig ) );

        assertEquals( 1, pipelines.size() );

        final Pipeline pipeline = pipelines.get( 0 );
        assertEquals( 0, pipeline.getOperatorIndex( operatorDef ) );
        assertEquals( RUNNING, pipeline.getPipelineStatus() );

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
        final List<Pipeline> pipelines = pipelineManager.createPipelines( supervisor, flow, asList( regionConfig1, regionConfig2 ) );

        assertEquals( 2, pipelines.size() );

        final Pipeline pipeline1 = pipelines.get( 0 );
        assertEquals( new UpstreamContext( 0, new UpstreamConnectionStatus[] { NO_CONNECTION, NO_CONNECTION } ),
                      pipeline1.getUpstreamContext() );
        assertEquals( partitionedStatefulRegionDef, pipeline1.getRegionDef() );
        assertEquals( 0, pipeline1.getOperatorIndex( operatorDef1 ) );
        assertNotEquals( pipeline1.getPipelineReplica( 0 ), pipeline1.getPipelineReplica( 1 ) );
        assertTrue( pipeline1.getDownstreamTupleSender( 0 ) instanceof DownstreamTupleSender1 );
        assertTrue( pipeline1.getDownstreamTupleSender( 1 ) instanceof DownstreamTupleSender1 );

        final Pipeline pipeline2 = pipelines.get( 1 );
        assertEquals( ( (Supplier<TupleQueueContext>) pipeline1.getDownstreamTupleSender( 0 ) ).get(),
                      pipeline2.getPipelineReplica( 0 ).getUpstreamTupleQueueContext() );
        assertEquals( ( (Supplier<TupleQueueContext>) pipeline1.getDownstreamTupleSender( 1 ) ).get(),
                      pipeline2.getPipelineReplica( 0 ).getUpstreamTupleQueueContext() );
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
        final List<Pipeline> pipelines = pipelineManager.createPipelines( supervisor,
                                                                          flow,
                                                                          asList( regionConfig1, regionConfig2, regionConfig3 ) );

        assertEquals( 3, pipelines.size() );

        final Pipeline pipeline1 = pipelines.get( 0 );
        final Pipeline pipeline2 = pipelines.get( 1 );
        final Pipeline pipeline3 = pipelines.get( 2 );

        assertEquals( new UpstreamContext( 0, new UpstreamConnectionStatus[] { NO_CONNECTION, NO_CONNECTION } ),
                      pipeline1.getUpstreamContext() );
        assertEquals( partitionedStatefulRegionDef, pipeline1.getRegionDef() );
        assertEquals( 0, pipeline1.getOperatorIndex( operatorDef1 ) );
        assertNotEquals( pipeline1.getPipelineReplica( 0 ), pipeline1.getPipelineReplica( 1 ) );
        assertTrue( pipeline1.getDownstreamTupleSender( 0 ) instanceof CompositeDownstreamTupleSender );
        assertTrue( pipeline1.getDownstreamTupleSender( 1 ) instanceof CompositeDownstreamTupleSender );
        final DownstreamTupleSender[] senders0 = ( (CompositeDownstreamTupleSender) pipeline1.getDownstreamTupleSender( 0 ) )
                                                         .getDownstreamTupleSenders();

        final DownstreamTupleSender[] senders1 = ( (CompositeDownstreamTupleSender) pipeline1.getDownstreamTupleSender( 1 ) )
                                                         .getDownstreamTupleSenders();

        if ( ( (Supplier<TupleQueueContext>) senders0[ 0 ] ).get()
                                                            .equals( pipeline2.getPipelineReplica( 0 ).getUpstreamTupleQueueContext() ) )
        {
            assertEquals( ( (Supplier<TupleQueueContext>) senders0[ 1 ] ).get(),
                          pipeline3.getPipelineReplica( 0 ).getUpstreamTupleQueueContext() );
        }
        else if ( ( (Supplier<TupleQueueContext>) senders0[ 1 ] ).get()
                                                                 .equals( pipeline2.getPipelineReplica( 0 )
                                                                                   .getUpstreamTupleQueueContext() ) )
        {
            assertEquals( ( (Supplier<TupleQueueContext>) senders0[ 0 ] ).get(),
                          pipeline3.getPipelineReplica( 0 ).getUpstreamTupleQueueContext() );
        }
        else
        {
            fail();
        }

        if ( ( (Supplier<TupleQueueContext>) senders1[ 0 ] ).get()
                                                            .equals( pipeline2.getPipelineReplica( 0 ).getUpstreamTupleQueueContext() ) )
        {
            assertEquals( ( (Supplier<TupleQueueContext>) senders1[ 1 ] ).get(),
                          pipeline3.getPipelineReplica( 1 ).getUpstreamTupleQueueContext() );
        }
        else if ( ( (Supplier<TupleQueueContext>) senders1[ 1 ] ).get()
                                                                 .equals( pipeline2.getPipelineReplica( 0 )
                                                                                   .getUpstreamTupleQueueContext() ) )
        {
            assertEquals( ( (Supplier<TupleQueueContext>) senders1[ 0 ] ).get(),
                          pipeline3.getPipelineReplica( 1 ).getUpstreamTupleQueueContext() );
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

        final List<Pipeline> pipelines = pipelineManager.createPipelines( supervisor, flow, asList( regionConfig1, regionConfig2 ) );

        assertEquals( 3, pipelines.size() );

        final Pipeline pipeline1 = pipelines.get( 0 );
        final Pipeline pipeline2 = pipelines.get( 1 );
        final Pipeline pipeline3 = pipelines.get( 2 );

        assertEquals( statefulRegionDef, pipeline1.getRegionDef() );
        assertEquals( 0, pipeline1.getOperatorIndex( operatorDef1 ) );
        assertTrue( pipeline1.getDownstreamTupleSender( 0 ) instanceof PartitionedDownstreamTupleSender1 );
        assertArrayEquals( ( (Supplier<TupleQueueContext[]>) pipeline1.getDownstreamTupleSender( 0 ) ).get(),
                           new TupleQueueContext[] { pipeline2.getPipelineReplica( 0 ).getUpstreamTupleQueueContext(),
                                                     pipeline2.getPipelineReplica( 1 ).getUpstreamTupleQueueContext() } );

        assertEquals( new UpstreamContext( 0, new UpstreamConnectionStatus[] { ACTIVE, NO_CONNECTION } ), pipeline2.getUpstreamContext() );
        assertEquals( partitionedStatefulRegionDef, pipeline2.getRegionDef() );
        assertEquals( 0, pipeline2.getOperatorIndex( operatorDef2 ) );
        assertFalse( pipeline2.getPipelineReplica( 0 ) == pipeline2.getPipelineReplica( 1 ) );
        assertTrue( pipeline2.getDownstreamTupleSender( 0 ) instanceof DownstreamTupleSender1 );
        assertTrue( pipeline2.getDownstreamTupleSender( 1 ) instanceof DownstreamTupleSender1 );
        assertEquals( ( (Supplier<TupleQueueContext>) pipeline2.getDownstreamTupleSender( 0 ) ).get(),
                      pipeline3.getPipelineReplica( 0 ).getUpstreamTupleQueueContext() );
        assertEquals( ( (Supplier<TupleQueueContext>) pipeline2.getDownstreamTupleSender( 1 ) ).get(),
                      pipeline3.getPipelineReplica( 1 ).getUpstreamTupleQueueContext() );

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

        final List<Pipeline> pipelines = pipelineManager.createPipelines( supervisor, flow, asList( regionConfig1, regionConfig2 ) );

        assertEquals( 3, pipelines.size() );

        final Pipeline pipeline1 = pipelines.get( 0 );
        final Pipeline pipeline2 = pipelines.get( 1 );
        final Pipeline pipeline3 = pipelines.get( 2 );

        assertEquals( statefulRegionDef, pipeline1.getRegionDef() );
        assertEquals( 0, pipeline1.getOperatorIndex( operatorDef1 ) );
        assertTrue( pipeline1.getDownstreamTupleSender( 0 ) instanceof PartitionedDownstreamTupleSender1 );
        assertArrayEquals( ( (Supplier<TupleQueueContext[]>) pipeline1.getDownstreamTupleSender( 0 ) ).get(),
                           new TupleQueueContext[] { pipeline2.getPipelineReplica( 0 ).getUpstreamTupleQueueContext(),
                                                     pipeline2.getPipelineReplica( 1 ).getUpstreamTupleQueueContext() } );

        assertEquals( new UpstreamContext( 0, new UpstreamConnectionStatus[] { ACTIVE } ), pipeline2.getUpstreamContext() );
        assertEquals( partitionedStatefulRegionDef, pipeline2.getRegionDef() );
        assertEquals( 0, pipeline2.getOperatorIndex( operatorDef2 ) );
        assertFalse( pipeline2.getPipelineReplica( 0 ) == pipeline2.getPipelineReplica( 1 ) );
        assertTrue( pipeline2.getDownstreamTupleSender( 0 ) instanceof DownstreamTupleSender1 );
        assertTrue( pipeline2.getDownstreamTupleSender( 1 ) instanceof DownstreamTupleSender1 );
        assertEquals( ( (Supplier<TupleQueueContext>) pipeline2.getDownstreamTupleSender( 0 ) ).get(),
                      pipeline3.getPipelineReplica( 0 ).getUpstreamTupleQueueContext() );
        assertEquals( ( (Supplier<TupleQueueContext>) pipeline2.getDownstreamTupleSender( 1 ) ).get(),
                      pipeline3.getPipelineReplica( 1 ).getUpstreamTupleQueueContext() );

        assertEquals( new UpstreamContext( 0, new UpstreamConnectionStatus[] { ACTIVE, NO_CONNECTION } ), pipeline3.getUpstreamContext() );
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

        final List<Pipeline> pipelines = pipelineManager.createPipelines( supervisor,
                                                                          flow,
                                                                          asList( regionConfig1, regionConfig2, regionConfig3 ) );

        final Pipeline pipeline1 = pipelines.get( 0 );
        final Pipeline pipeline2 = pipelines.get( 1 );
        final Pipeline pipeline3 = pipelines.get( 2 );

        assertEquals( statefulRegionDef1, pipeline1.getRegionDef() );
        assertEquals( 0, pipeline1.getOperatorIndex( statefulRegionDef1.getOperators().get( 0 ) ) );
        assertTrue( pipeline1.getDownstreamTupleSender( 0 ) instanceof DownstreamTupleSender1 );
        assertEquals( ( (Supplier<TupleQueueContext>) pipeline1.getDownstreamTupleSender( 0 ) ).get(),
                      pipeline3.getPipelineReplica( 0 ).getUpstreamTupleQueueContext() );

        assertEquals( statefulRegionDef2, pipeline2.getRegionDef() );
        assertEquals( 0, pipeline2.getOperatorIndex( statefulRegionDef2.getOperators().get( 0 ) ) );
        assertTrue( pipeline2.getDownstreamTupleSender( 0 ) instanceof DownstreamTupleSender1 );
        assertEquals( ( (Supplier<TupleQueueContext>) pipeline2.getDownstreamTupleSender( 0 ) ).get(),
                      pipeline3.getPipelineReplica( 0 ).getUpstreamTupleQueueContext() );

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

        final List<Pipeline> pipelines = pipelineManager.createPipelines( supervisor,
                                                                          flow,
                                                                          asList( regionConfig1, regionConfig2, regionConfig3 ) );

        final Pipeline pipeline1 = pipelines.get( 0 );
        final Pipeline pipeline2 = pipelines.get( 1 );
        final Pipeline pipeline3 = pipelines.get( 2 );

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
    @OperatorSchema( inputs = {}, outputs = { @PortSchema( portIndex = 0, scope = EXACT_FIELD_SET, fields = { @SchemaField( name = "field1", type = Integer.class ) } ) } )
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
