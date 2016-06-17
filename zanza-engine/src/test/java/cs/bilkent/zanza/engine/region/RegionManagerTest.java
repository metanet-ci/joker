package cs.bilkent.zanza.engine.region;

import java.util.List;

import org.junit.Before;
import org.junit.Test;

import cs.bilkent.zanza.engine.config.ThreadingPreference;
import static cs.bilkent.zanza.engine.config.ThreadingPreference.MULTI_THREADED;
import static cs.bilkent.zanza.engine.config.ThreadingPreference.SINGLE_THREADED;
import cs.bilkent.zanza.engine.config.ZanzaConfig;
import cs.bilkent.zanza.engine.kvstore.KVStoreContext;
import cs.bilkent.zanza.engine.kvstore.impl.DefaultKVStoreContext;
import cs.bilkent.zanza.engine.kvstore.impl.EmptyKVStoreContext;
import cs.bilkent.zanza.engine.kvstore.impl.KVStoreContextManagerImpl;
import cs.bilkent.zanza.engine.kvstore.impl.PartitionedKVStoreContext;
import cs.bilkent.zanza.engine.partition.PartitionService;
import cs.bilkent.zanza.engine.partition.PartitionServiceImpl;
import cs.bilkent.zanza.engine.pipeline.OperatorInstance;
import cs.bilkent.zanza.engine.pipeline.PipelineInstance;
import cs.bilkent.zanza.engine.pipeline.PipelineInstanceId;
import cs.bilkent.zanza.engine.pipeline.impl.tuplesupplier.CachedTuplesImplSupplier;
import cs.bilkent.zanza.engine.pipeline.impl.tuplesupplier.NonCachedTuplesImplSupplier;
import cs.bilkent.zanza.engine.region.impl.RegionFormerImpl;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueContext;
import cs.bilkent.zanza.engine.tuplequeue.impl.TupleQueueContextManagerImpl;
import cs.bilkent.zanza.engine.tuplequeue.impl.context.DefaultTupleQueueContext;
import cs.bilkent.zanza.engine.tuplequeue.impl.context.EmptyTupleQueueContext;
import cs.bilkent.zanza.engine.tuplequeue.impl.context.PartitionedTupleQueueContext;
import cs.bilkent.zanza.engine.tuplequeue.impl.drainer.pool.BlockingTupleQueueDrainerPool;
import cs.bilkent.zanza.engine.tuplequeue.impl.drainer.pool.NonBlockingTupleQueueDrainerPool;
import cs.bilkent.zanza.flow.FlowDefinition;
import cs.bilkent.zanza.flow.FlowDefinitionBuilder;
import cs.bilkent.zanza.flow.OperatorDefinition;
import cs.bilkent.zanza.flow.OperatorDefinitionBuilder;
import cs.bilkent.zanza.flow.OperatorRuntimeSchemaBuilder;
import cs.bilkent.zanza.operator.InitializationContext;
import cs.bilkent.zanza.operator.InvocationContext;
import cs.bilkent.zanza.operator.Operator;
import cs.bilkent.zanza.operator.scheduling.SchedulingStrategy;
import cs.bilkent.zanza.operator.spec.OperatorSpec;
import cs.bilkent.zanza.operator.spec.OperatorType;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class RegionManagerTest
{

    private final PartitionService partitionService = new PartitionServiceImpl();

    private final KVStoreContextManagerImpl kvStoreContextManager = new KVStoreContextManagerImpl();

    private final TupleQueueContextManagerImpl tupleQueueContextManager = new TupleQueueContextManagerImpl();

    private final RegionManager regionManager = new RegionManager();

    @Before
    public void init ()
    {
        final ZanzaConfig config = new ZanzaConfig();
        partitionService.init( config );
        kvStoreContextManager.setPartitionService( partitionService );
        tupleQueueContextManager.setPartitionService( partitionService );
        tupleQueueContextManager.init( config );
        regionManager.setKvStoreContextManager( kvStoreContextManager );
        regionManager.setTupleQueueContextManager( tupleQueueContextManager );
    }

    @Test
    public void test_statelessRegion_singlePipeline_singleReplica_noInputConnection ()
    {

        final Class<StatelessOperatorWithSingleInputOutputPortCount> operatorClazz = StatelessOperatorWithSingleInputOutputPortCount.class;
        final OperatorDefinition operatorDefinition0 = OperatorDefinitionBuilder.newInstance( "op0", operatorClazz ).build();
        final OperatorDefinition operatorDefinition1 = OperatorDefinitionBuilder.newInstance( "op1", operatorClazz ).build();

        final FlowDefinition flow = new FlowDefinitionBuilder().add( operatorDefinition0 )
                                                               .add( operatorDefinition1 )
                                                               .connect( "op0", "op1" )
                                                               .build();

        final List<RegionDefinition> regions = new RegionFormerImpl().createRegions( flow );
        final RegionDefinition region = regions.get( 0 );
        final RegionRuntimeConfig regionConfig = new RegionRuntimeConfig( 1, region, 1, singletonList( 0 ) );

        final RegionInstance regionInstance = regionManager.createRegion( flow, regionConfig );
        assertNotNull( regionInstance );
        final PipelineInstance[] pipelines = regionInstance.getPipelines( 0 );
        assertEquals( 1, pipelines.length );
        final PipelineInstance pipeline = pipelines[ 0 ];
        assertEmptyTupleQueueContext( pipeline, operatorDefinition0.inputPortCount() );

        assertStatelessPipelineWithNoInput( 1, 0, 0, operatorDefinition0, operatorDefinition1, pipeline );
    }

    @Test
    public void test_statelessRegion_singlePipeline_multiReplica_noInputConnection ()
    {

        final Class<StatelessOperatorWithSingleInputOutputPortCount> operatorClazz = StatelessOperatorWithSingleInputOutputPortCount.class;
        final OperatorDefinition operatorDefinition0 = OperatorDefinitionBuilder.newInstance( "op0", operatorClazz ).build();
        final OperatorDefinition operatorDefinition1 = OperatorDefinitionBuilder.newInstance( "op1", operatorClazz ).build();

        final FlowDefinition flow = new FlowDefinitionBuilder().add( operatorDefinition0 )
                                                               .add( operatorDefinition1 )
                                                               .connect( "op0", "op1" )
                                                               .build();

        final List<RegionDefinition> regions = new RegionFormerImpl().createRegions( flow );
        final RegionDefinition region = regions.get( 0 );
        final RegionRuntimeConfig regionConfig = new RegionRuntimeConfig( 1, region, 2, singletonList( 0 ) );

        final RegionInstance regionInstance = regionManager.createRegion( flow, regionConfig );
        assertNotNull( regionInstance );
        final PipelineInstance[] pipelines0 = regionInstance.getPipelines( 0 );
        final PipelineInstance[] pipelines1 = regionInstance.getPipelines( 1 );
        assertEquals( 1, pipelines0.length );
        final PipelineInstance pipelineReplica0 = pipelines0[ 0 ];
        final PipelineInstance pipelineReplica1 = pipelines1[ 0 ];
        assertEmptyTupleQueueContext( pipelineReplica0, operatorDefinition0.inputPortCount() );
        assertEmptyTupleQueueContext( pipelineReplica1, operatorDefinition0.inputPortCount() );

        assertStatelessPipelineWithNoInput( 1, 0, 0, operatorDefinition0, operatorDefinition1, pipelineReplica0 );
        assertStatelessPipelineWithNoInput( 1, 1, 0, operatorDefinition0, operatorDefinition1, pipelineReplica1 );
    }

    @Test
    public void test_statelessRegion_multiPipeline_singleReplica_noInputConnection ()
    {

        final Class<StatelessOperatorWithSingleInputOutputPortCount> operatorClazz = StatelessOperatorWithSingleInputOutputPortCount.class;
        final OperatorDefinition operatorDefinition0 = OperatorDefinitionBuilder.newInstance( "op0", operatorClazz ).build();
        final OperatorDefinition operatorDefinition1 = OperatorDefinitionBuilder.newInstance( "op1", operatorClazz ).build();

        final FlowDefinition flow = new FlowDefinitionBuilder().add( operatorDefinition0 )
                                                               .add( operatorDefinition1 )
                                                               .connect( "op0", "op1" )
                                                               .build();

        final List<RegionDefinition> regions = new RegionFormerImpl().createRegions( flow );
        final RegionDefinition region = regions.get( 0 );
        final RegionRuntimeConfig regionConfig = new RegionRuntimeConfig( 1, region, 1, asList( 0, 1 ) );

        final RegionInstance regionInstance = regionManager.createRegion( flow, regionConfig );
        assertNotNull( regionInstance );
        final PipelineInstance[] pipelines = regionInstance.getPipelines( 0 );
        assertEquals( 2, pipelines.length );
        final PipelineInstance pipeline0 = pipelines[ 0 ];
        final PipelineInstance pipeline1 = pipelines[ 1 ];
        assertEmptyTupleQueueContext( pipeline0, operatorDefinition0.inputPortCount() );
        assertEmptyTupleQueueContext( pipeline1, operatorDefinition1.inputPortCount() );

        assertEquals( new PipelineInstanceId( 1, 0, 0 ), pipeline0.id() );
        assertEquals( new PipelineInstanceId( 1, 0, 1 ), pipeline1.id() );
        assertEquals( 1, pipeline0.getOperatorCount() );
        assertEquals( 1, pipeline1.getOperatorCount() );
        final OperatorInstance operatorInstance0 = pipeline0.getOperatorInstance( 0 );
        final OperatorInstance operatorInstance1 = pipeline1.getOperatorInstance( 0 );
        assertOperatorDefinition( operatorInstance0, operatorDefinition0 );
        assertOperatorDefinition( operatorInstance1, operatorDefinition1 );
        assertEmptyTupleQueueContext( operatorInstance0 );
        assertDefaultTupleQueueContext( operatorInstance1, operatorDefinition1.inputPortCount(), MULTI_THREADED );
        assertEmptyKVStoreContext( operatorInstance0 );
        assertEmptyKVStoreContext( operatorInstance1 );
        assertBlockingTupleQueueDrainerPool( operatorInstance0 );
        assertBlockingTupleQueueDrainerPool( operatorInstance1 );
        assertNonCachedTuplesImplSupplier( operatorInstance0 );
        assertNonCachedTuplesImplSupplier( operatorInstance1 );
    }

    private void assertStatelessPipelineWithNoInput ( final int regionId,
                                                      final int replicaIndex,
                                                      final int pipelineId,
                                                      final OperatorDefinition operatorDefinition0,
                                                      final OperatorDefinition operatorDefinition1,
                                                      final PipelineInstance pipelineReplica0 )
    {
        assertEquals( new PipelineInstanceId( regionId, replicaIndex, pipelineId ), pipelineReplica0.id() );
        assertEquals( 2, pipelineReplica0.getOperatorCount() );
        final OperatorInstance operatorInstance0 = pipelineReplica0.getOperatorInstance( 0 );
        final OperatorInstance operatorInstance1 = pipelineReplica0.getOperatorInstance( 1 );
        assertOperatorDefinition( operatorInstance0, operatorDefinition0 );
        assertOperatorDefinition( operatorInstance1, operatorDefinition1 );
        assertEmptyTupleQueueContext( operatorInstance0 );
        assertDefaultTupleQueueContext( operatorInstance1, operatorDefinition1.inputPortCount(), SINGLE_THREADED );
        assertEmptyKVStoreContext( operatorInstance0 );
        assertEmptyKVStoreContext( operatorInstance1 );
        assertBlockingTupleQueueDrainerPool( operatorInstance0 );
        assertNonBlockingTupleQueueDrainerPool( operatorInstance1 );
        assertCachedTuplesImplSupplier( operatorInstance0 );
        assertNonCachedTuplesImplSupplier( operatorInstance1 );
    }

    @Test
    public void test_statelessRegion_singlePipeline_singleReplica_withInputConnection ()
    {

        final Class<StatelessOperatorWithSingleInputOutputPortCount> operatorClazz = StatelessOperatorWithSingleInputOutputPortCount.class;
        final OperatorDefinition operatorDefinition0 = OperatorDefinitionBuilder.newInstance( "op0",
                                                                                              StatefulOperatorWithSingleInputOutputPortCount.class )
                                                                                .build();
        final OperatorDefinition operatorDefinition1 = OperatorDefinitionBuilder.newInstance( "op1", operatorClazz ).build();
        final OperatorDefinition operatorDefinition2 = OperatorDefinitionBuilder.newInstance( "op2", operatorClazz ).build();

        final FlowDefinition flow = new FlowDefinitionBuilder().add( operatorDefinition0 )
                                                               .add( operatorDefinition1 )
                                                               .add( operatorDefinition2 )
                                                               .connect( "op0", "op1" )
                                                               .connect( "op1", "op2" )
                                                               .build();

        final List<RegionDefinition> regions = new RegionFormerImpl().createRegions( flow );
        final RegionDefinition region = regions.get( 1 );
        final RegionRuntimeConfig regionConfig = new RegionRuntimeConfig( 1, region, 1, singletonList( 0 ) );

        final RegionInstance regionInstance = regionManager.createRegion( flow, regionConfig );
        assertNotNull( regionInstance );
        final PipelineInstance[] pipelines = regionInstance.getPipelines( 0 );
        assertEquals( 1, pipelines.length );
        final PipelineInstance pipeline = pipelines[ 0 ];
        assertEmptyTupleQueueContext( pipeline, operatorDefinition1.inputPortCount() );

        assertEquals( new PipelineInstanceId( 1, 0, 0 ), pipeline.id() );
        assertEquals( 2, pipeline.getOperatorCount() );
        final OperatorInstance operatorInstance1 = pipeline.getOperatorInstance( 0 );
        final OperatorInstance operatorInstance2 = pipeline.getOperatorInstance( 1 );
        assertOperatorDefinition( operatorInstance1, operatorDefinition1 );
        assertOperatorDefinition( operatorInstance2, operatorDefinition2 );
        assertDefaultTupleQueueContext( operatorInstance1, operatorDefinition1.inputPortCount(), MULTI_THREADED );
        assertDefaultTupleQueueContext( operatorInstance2, operatorDefinition2.inputPortCount(), SINGLE_THREADED );
        assertEmptyKVStoreContext( operatorInstance1 );
        assertEmptyKVStoreContext( operatorInstance2 );
        assertBlockingTupleQueueDrainerPool( operatorInstance1 );
        assertNonBlockingTupleQueueDrainerPool( operatorInstance2 );
        assertCachedTuplesImplSupplier( operatorInstance1 );
        assertNonCachedTuplesImplSupplier( operatorInstance2 );
    }

    @Test
    public void test_statefulRegion_singlePipeline_singleReplica ()
    {
        final OperatorDefinition operatorDefinition0 = OperatorDefinitionBuilder.newInstance( "op0",
                                                                                              StatelessOperatorWithSingleInputOutputPortCount.class )
                                                                                .build();
        final OperatorDefinition operatorDefinition1 = OperatorDefinitionBuilder.newInstance( "op1",
                                                                                              StatefulOperatorWithSingleInputOutputPortCount.class )
                                                                                .build();

        final FlowDefinition flow = new FlowDefinitionBuilder().add( operatorDefinition0 )
                                                               .add( operatorDefinition1 )
                                                               .connect( "op0", "op1" )
                                                               .build();

        final List<RegionDefinition> regions = new RegionFormerImpl().createRegions( flow );
        final RegionDefinition region = regions.get( 1 );
        final RegionRuntimeConfig regionConfig = new RegionRuntimeConfig( 1, region, 1, singletonList( 0 ) );

        final RegionInstance regionInstance = regionManager.createRegion( flow, regionConfig );
        assertNotNull( regionInstance );
        final PipelineInstance[] pipelines = regionInstance.getPipelines( 0 );
        assertEquals( 1, pipelines.length );
        final PipelineInstance pipeline = pipelines[ 0 ];
        assertEmptyTupleQueueContext( pipeline, operatorDefinition1.inputPortCount() );

        assertEquals( new PipelineInstanceId( 1, 0, 0 ), pipeline.id() );
        assertEquals( 1, pipeline.getOperatorCount() );

        final OperatorInstance operatorInstance1 = pipeline.getOperatorInstance( 0 );
        assertOperatorDefinition( operatorInstance1, operatorDefinition1 );
        assertDefaultTupleQueueContext( operatorInstance1, operatorDefinition1.inputPortCount(), MULTI_THREADED );
        assertDefaultKVStoreContext( operatorInstance1 );
        assertBlockingTupleQueueDrainerPool( operatorInstance1 );
        assertNonCachedTuplesImplSupplier( operatorInstance1 );
    }

    @Test
    public void test_partitionedStatefulRegion_singlePipeline_singleReplica ()
    {
        final OperatorRuntimeSchemaBuilder operatorRuntimeSchemaBuilder0 = new OperatorRuntimeSchemaBuilder( 3, 1 );
        operatorRuntimeSchemaBuilder0.getInputPortSchemaBuilder( 0 ).addField( "field1", Integer.class );
        operatorRuntimeSchemaBuilder0.getOutputPortSchemaBuilder( 0 ).addField( "field2", Integer.class );
        final OperatorRuntimeSchemaBuilder operatorRuntimeSchemaBuilder1 = new OperatorRuntimeSchemaBuilder( 2, 1 );
        operatorRuntimeSchemaBuilder1.getInputPortSchemaBuilder( 0 ).addField( "field2", Integer.class );
        operatorRuntimeSchemaBuilder1.getInputPortSchemaBuilder( 1 ).addField( "field2", Integer.class );
        operatorRuntimeSchemaBuilder1.getOutputPortSchemaBuilder( 0 ).addField( "field3", Integer.class );
        final OperatorDefinition operatorDefinition0 = OperatorDefinitionBuilder.newInstance( "op0",
                                                                                              StatefulOperatorWithSingleInputOutputPortCount.class )
                                                                                .setExtendingSchema( operatorRuntimeSchemaBuilder0 )
                                                                                .build();
        final OperatorDefinition operatorDefinition1 = OperatorDefinitionBuilder.newInstance( "op1",
                                                                                              PartitionedStatefulOperatorWithSingleInputOutputPortCount.class )
                                                                                .setExtendingSchema( operatorRuntimeSchemaBuilder1 )
                                                                                .setPartitionFieldNames( singletonList( "field2" ) )
                                                                                .build();

        final FlowDefinition flow = new FlowDefinitionBuilder().add( operatorDefinition0 )
                                                               .add( operatorDefinition1 )
                                                               .connect( "op0", "op1" )
                                                               .build();

        final List<RegionDefinition> regions = new RegionFormerImpl().createRegions( flow );
        final RegionDefinition region = regions.get( 1 );
        final RegionRuntimeConfig regionConfig = new RegionRuntimeConfig( 1, region, 1, singletonList( 0 ) );

        final RegionInstance regionInstance = regionManager.createRegion( flow, regionConfig );
        assertNotNull( regionInstance );
        final PipelineInstance[] pipelines = regionInstance.getPipelines( 0 );
        assertEquals( 1, pipelines.length );
        final PipelineInstance pipeline = pipelines[ 0 ];
        assertDefaultTupleQueueContext( pipeline, operatorDefinition1.inputPortCount() );

        assertEquals( new PipelineInstanceId( 1, 0, 0 ), pipeline.id() );
        assertEquals( 1, pipeline.getOperatorCount() );

        final OperatorInstance operatorInstance1 = pipeline.getOperatorInstance( 0 );
        assertOperatorDefinition( operatorInstance1, operatorDefinition1 );
        assertPartitionedTupleQueueContext( operatorInstance1 );
        assertPartitionedKVStoreContext( operatorInstance1 );
        assertBlockingTupleQueueDrainerPool( operatorInstance1 );
        assertNonCachedTuplesImplSupplier( operatorInstance1 );
    }

    @Test
    public void test_partitionedStatefulRegion_singlePipeline_multiReplica ()
    {
        final OperatorRuntimeSchemaBuilder operatorRuntimeSchemaBuilder0 = new OperatorRuntimeSchemaBuilder( 3, 1 );
        operatorRuntimeSchemaBuilder0.getInputPortSchemaBuilder( 0 ).addField( "field1", Integer.class );
        operatorRuntimeSchemaBuilder0.getOutputPortSchemaBuilder( 0 ).addField( "field2", Integer.class );
        final OperatorRuntimeSchemaBuilder operatorRuntimeSchemaBuilder1 = new OperatorRuntimeSchemaBuilder( 2, 1 );
        operatorRuntimeSchemaBuilder1.getInputPortSchemaBuilder( 0 ).addField( "field2", Integer.class );
        operatorRuntimeSchemaBuilder1.getInputPortSchemaBuilder( 1 ).addField( "field2", Integer.class );
        operatorRuntimeSchemaBuilder1.getOutputPortSchemaBuilder( 0 ).addField( "field3", Integer.class );
        final OperatorDefinition operatorDefinition0 = OperatorDefinitionBuilder.newInstance( "op0",
                                                                                              StatefulOperatorWithSingleInputOutputPortCount.class )
                                                                                .setExtendingSchema( operatorRuntimeSchemaBuilder0 )
                                                                                .build();
        final OperatorDefinition operatorDefinition1 = OperatorDefinitionBuilder.newInstance( "op1",
                                                                                              PartitionedStatefulOperatorWithSingleInputOutputPortCount.class )
                                                                                .setExtendingSchema( operatorRuntimeSchemaBuilder1 )
                                                                                .setPartitionFieldNames( singletonList( "field2" ) )
                                                                                .build();

        final FlowDefinition flow = new FlowDefinitionBuilder().add( operatorDefinition0 )
                                                               .add( operatorDefinition1 )
                                                               .connect( "op0", "op1" )
                                                               .build();

        final List<RegionDefinition> regions = new RegionFormerImpl().createRegions( flow );
        final RegionDefinition region = regions.get( 1 );
        final RegionRuntimeConfig regionConfig = new RegionRuntimeConfig( 1, region, 2, singletonList( 0 ) );

        final RegionInstance regionInstance = regionManager.createRegion( flow, regionConfig );
        assertNotNull( regionInstance );
        final PipelineInstance[] pipelines0 = regionInstance.getPipelines( 0 );
        final PipelineInstance[] pipelines1 = regionInstance.getPipelines( 1 );
        assertEquals( 1, pipelines0.length );
        assertEquals( 1, pipelines1.length );
        final PipelineInstance pipelineReplica0 = pipelines0[ 0 ];
        final PipelineInstance pipelineReplica1 = pipelines1[ 0 ];
        assertDefaultTupleQueueContext( pipelineReplica0, operatorDefinition1.inputPortCount() );
        assertDefaultTupleQueueContext( pipelineReplica1, operatorDefinition1.inputPortCount() );

        assertEquals( new PipelineInstanceId( 1, 0, 0 ), pipelineReplica0.id() );
        assertEquals( new PipelineInstanceId( 1, 1, 0 ), pipelineReplica1.id() );
        assertEquals( 1, pipelineReplica0.getOperatorCount() );
        assertEquals( 1, pipelineReplica1.getOperatorCount() );

        final OperatorInstance operatorInstanceReplica0 = pipelineReplica0.getOperatorInstance( 0 );
        final OperatorInstance operatorInstanceReplica1 = pipelineReplica1.getOperatorInstance( 0 );
        assertOperatorDefinition( operatorInstanceReplica0, operatorDefinition1 );
        assertOperatorDefinition( operatorInstanceReplica1, operatorDefinition1 );
        assertPartitionedTupleQueueContext( operatorInstanceReplica0 );
        assertPartitionedTupleQueueContext( operatorInstanceReplica1 );
        assertPartitionedKVStoreContext( operatorInstanceReplica0 );
        assertPartitionedKVStoreContext( operatorInstanceReplica1 );
        assertBlockingTupleQueueDrainerPool( operatorInstanceReplica0 );
        assertBlockingTupleQueueDrainerPool( operatorInstanceReplica1 );
        assertNonCachedTuplesImplSupplier( operatorInstanceReplica0 );
        assertNonCachedTuplesImplSupplier( operatorInstanceReplica1 );
    }

    @Test
    public void test_partitionedStatefulRegion_singlePipeline_singleReplica_withStatelessOperatorFirst ()
    {
        final OperatorRuntimeSchemaBuilder operatorRuntimeSchemaBuilder0 = new OperatorRuntimeSchemaBuilder( 3, 1 );
        operatorRuntimeSchemaBuilder0.getInputPortSchemaBuilder( 0 ).addField( "field1", Integer.class );
        operatorRuntimeSchemaBuilder0.getOutputPortSchemaBuilder( 0 ).addField( "field2", Integer.class );
        final OperatorRuntimeSchemaBuilder operatorRuntimeSchemaBuilder1 = new OperatorRuntimeSchemaBuilder( 1, 1 );
        operatorRuntimeSchemaBuilder1.getInputPortSchemaBuilder( 0 ).addField( "field2", Integer.class );
        operatorRuntimeSchemaBuilder1.getOutputPortSchemaBuilder( 0 ).addField( "field2", Integer.class );
        final OperatorRuntimeSchemaBuilder operatorRuntimeSchemaBuilder2 = new OperatorRuntimeSchemaBuilder( 2, 1 );
        operatorRuntimeSchemaBuilder2.getInputPortSchemaBuilder( 0 ).addField( "field2", Integer.class );
        operatorRuntimeSchemaBuilder2.getInputPortSchemaBuilder( 1 ).addField( "field2", Integer.class );
        operatorRuntimeSchemaBuilder2.getOutputPortSchemaBuilder( 0 ).addField( "field3", Integer.class );
        final OperatorDefinition operatorDefinition0 = OperatorDefinitionBuilder.newInstance( "op0",
                                                                                              StatefulOperatorWithSingleInputOutputPortCount.class )
                                                                                .setExtendingSchema( operatorRuntimeSchemaBuilder0 )
                                                                                .build();
        final OperatorDefinition operatorDefinition1 = OperatorDefinitionBuilder.newInstance( "op1",
                                                                                              StatelessOperatorWithSingleInputOutputPortCount.class )
                                                                                .setExtendingSchema( operatorRuntimeSchemaBuilder1 )
                                                                                .build();
        final OperatorDefinition operatorDefinition2 = OperatorDefinitionBuilder.newInstance( "op2",
                                                                                              PartitionedStatefulOperatorWithSingleInputOutputPortCount.class )
                                                                                .setExtendingSchema( operatorRuntimeSchemaBuilder2 )
                                                                                .setPartitionFieldNames( singletonList( "field2" ) )
                                                                                .build();

        final FlowDefinition flow = new FlowDefinitionBuilder().add( operatorDefinition0 )
                                                               .add( operatorDefinition1 )
                                                               .add( operatorDefinition2 )
                                                               .connect( "op0", "op1" )
                                                               .connect( "op1", "op2" )
                                                               .build();

        final List<RegionDefinition> regions = new RegionFormerImpl().createRegions( flow );
        final RegionDefinition region = regions.get( 1 );
        final RegionRuntimeConfig regionConfig = new RegionRuntimeConfig( 1, region, 1, singletonList( 0 ) );

        final RegionInstance regionInstance = regionManager.createRegion( flow, regionConfig );
        assertNotNull( regionInstance );
        final PipelineInstance[] pipelines = regionInstance.getPipelines( 0 );
        assertEquals( 1, pipelines.length );
        final PipelineInstance pipeline = pipelines[ 0 ];
        assertEmptyTupleQueueContext( pipeline, operatorDefinition1.inputPortCount() );

        assertEquals( new PipelineInstanceId( 1, 0, 0 ), pipeline.id() );
        assertEquals( 2, pipeline.getOperatorCount() );

        final OperatorInstance operatorInstance1 = pipeline.getOperatorInstance( 0 );
        assertOperatorDefinition( operatorInstance1, operatorDefinition1 );
        assertDefaultTupleQueueContext( operatorInstance1, operatorDefinition1.inputPortCount(), MULTI_THREADED );
        assertEmptyKVStoreContext( operatorInstance1 );
        assertBlockingTupleQueueDrainerPool( operatorInstance1 );
        assertCachedTuplesImplSupplier( operatorInstance1 );

        final OperatorInstance operatorInstance2 = pipeline.getOperatorInstance( 1 );
        assertOperatorDefinition( operatorInstance2, operatorDefinition2 );
        assertPartitionedTupleQueueContext( operatorInstance2 );
        assertPartitionedKVStoreContext( operatorInstance2 );
        assertNonBlockingTupleQueueDrainerPool( operatorInstance2 );
        assertNonCachedTuplesImplSupplier( operatorInstance2 );
    }

    @Test
    public void test_partitionedStatefulRegion_singlePipeline_multiReplica_withStatelessOperatorFirst ()
    {
        final OperatorRuntimeSchemaBuilder operatorRuntimeSchemaBuilder0 = new OperatorRuntimeSchemaBuilder( 3, 1 );
        operatorRuntimeSchemaBuilder0.getInputPortSchemaBuilder( 0 ).addField( "field1", Integer.class );
        operatorRuntimeSchemaBuilder0.getOutputPortSchemaBuilder( 0 ).addField( "field2", Integer.class );
        final OperatorRuntimeSchemaBuilder operatorRuntimeSchemaBuilder1 = new OperatorRuntimeSchemaBuilder( 1, 1 );
        operatorRuntimeSchemaBuilder1.getInputPortSchemaBuilder( 0 ).addField( "field2", Integer.class );
        operatorRuntimeSchemaBuilder1.getOutputPortSchemaBuilder( 0 ).addField( "field2", Integer.class );
        final OperatorRuntimeSchemaBuilder operatorRuntimeSchemaBuilder2 = new OperatorRuntimeSchemaBuilder( 2, 1 );
        operatorRuntimeSchemaBuilder2.getInputPortSchemaBuilder( 0 ).addField( "field2", Integer.class );
        operatorRuntimeSchemaBuilder2.getInputPortSchemaBuilder( 1 ).addField( "field2", Integer.class );
        operatorRuntimeSchemaBuilder2.getOutputPortSchemaBuilder( 0 ).addField( "field3", Integer.class );
        final OperatorDefinition operatorDefinition0 = OperatorDefinitionBuilder.newInstance( "op0",
                                                                                              StatefulOperatorWithSingleInputOutputPortCount.class )
                                                                                .setExtendingSchema( operatorRuntimeSchemaBuilder0 )
                                                                                .build();
        final OperatorDefinition operatorDefinition1 = OperatorDefinitionBuilder.newInstance( "op1",
                                                                                              StatelessOperatorWithSingleInputOutputPortCount.class )
                                                                                .setExtendingSchema( operatorRuntimeSchemaBuilder1 )
                                                                                .build();
        final OperatorDefinition operatorDefinition2 = OperatorDefinitionBuilder.newInstance( "op2",
                                                                                              PartitionedStatefulOperatorWithSingleInputOutputPortCount.class )
                                                                                .setExtendingSchema( operatorRuntimeSchemaBuilder2 )
                                                                                .setPartitionFieldNames( singletonList( "field2" ) )
                                                                                .build();

        final FlowDefinition flow = new FlowDefinitionBuilder().add( operatorDefinition0 )
                                                               .add( operatorDefinition1 )
                                                               .add( operatorDefinition2 )
                                                               .connect( "op0", "op1" )
                                                               .connect( "op1", "op2" )
                                                               .build();

        final List<RegionDefinition> regions = new RegionFormerImpl().createRegions( flow );
        final RegionDefinition region = regions.get( 1 );
        final RegionRuntimeConfig regionConfig = new RegionRuntimeConfig( 1, region, 2, singletonList( 0 ) );

        final RegionInstance regionInstance = regionManager.createRegion( flow, regionConfig );
        assertNotNull( regionInstance );
        final PipelineInstance[] pipelinesReplica0 = regionInstance.getPipelines( 0 );
        final PipelineInstance[] pipelinesReplica1 = regionInstance.getPipelines( 1 );
        assertEquals( 1, pipelinesReplica0.length );
        assertEquals( 1, pipelinesReplica1.length );
        final PipelineInstance pipelineReplica0 = pipelinesReplica0[ 0 ];
        final PipelineInstance pipelineReplica1 = pipelinesReplica1[ 0 ];
        assertEmptyTupleQueueContext( pipelineReplica0, operatorDefinition1.inputPortCount() );
        assertEmptyTupleQueueContext( pipelineReplica1, operatorDefinition1.inputPortCount() );

        assertEquals( new PipelineInstanceId( 1, 0, 0 ), pipelineReplica0.id() );
        assertEquals( new PipelineInstanceId( 1, 1, 0 ), pipelineReplica1.id() );
        assertEquals( 2, pipelineReplica0.getOperatorCount() );
        assertEquals( 2, pipelineReplica1.getOperatorCount() );

        final OperatorInstance operatorInstance0_1 = pipelineReplica0.getOperatorInstance( 0 );
        assertOperatorDefinition( operatorInstance0_1, operatorDefinition1 );
        assertDefaultTupleQueueContext( operatorInstance0_1, operatorDefinition1.inputPortCount(), MULTI_THREADED );
        assertEmptyKVStoreContext( operatorInstance0_1 );
        assertBlockingTupleQueueDrainerPool( operatorInstance0_1 );
        assertCachedTuplesImplSupplier( operatorInstance0_1 );

        final OperatorInstance operatorInstance1_1 = pipelineReplica0.getOperatorInstance( 0 );
        assertOperatorDefinition( operatorInstance1_1, operatorDefinition1 );
        assertDefaultTupleQueueContext( operatorInstance1_1, operatorDefinition1.inputPortCount(), MULTI_THREADED );
        assertEmptyKVStoreContext( operatorInstance1_1 );
        assertBlockingTupleQueueDrainerPool( operatorInstance1_1 );
        assertCachedTuplesImplSupplier( operatorInstance1_1 );

        final OperatorInstance operatorInstance0_2 = pipelineReplica0.getOperatorInstance( 1 );
        assertOperatorDefinition( operatorInstance0_2, operatorDefinition2 );
        assertPartitionedTupleQueueContext( operatorInstance0_2 );
        assertPartitionedKVStoreContext( operatorInstance0_2 );
        assertNonBlockingTupleQueueDrainerPool( operatorInstance0_2 );
        assertNonCachedTuplesImplSupplier( operatorInstance0_2 );

        final OperatorInstance operatorInstance1_2 = pipelineReplica0.getOperatorInstance( 1 );
        assertOperatorDefinition( operatorInstance1_2, operatorDefinition2 );
        assertPartitionedTupleQueueContext( operatorInstance1_2 );
        assertPartitionedKVStoreContext( operatorInstance1_2 );
        assertNonBlockingTupleQueueDrainerPool( operatorInstance1_2 );
        assertNonCachedTuplesImplSupplier( operatorInstance1_2 );
    }

    @Test
    public void test_partitionedStatefulRegion_multiPipeline_singleReplica_withStatelessOperatorFirst ()
    {
        final OperatorRuntimeSchemaBuilder operatorRuntimeSchemaBuilder0 = new OperatorRuntimeSchemaBuilder( 3, 1 );
        operatorRuntimeSchemaBuilder0.getInputPortSchemaBuilder( 0 ).addField( "field1", Integer.class );
        operatorRuntimeSchemaBuilder0.getOutputPortSchemaBuilder( 0 ).addField( "field2", Integer.class );
        final OperatorRuntimeSchemaBuilder operatorRuntimeSchemaBuilder1 = new OperatorRuntimeSchemaBuilder( 1, 1 );
        operatorRuntimeSchemaBuilder1.getInputPortSchemaBuilder( 0 ).addField( "field2", Integer.class );
        operatorRuntimeSchemaBuilder1.getOutputPortSchemaBuilder( 0 ).addField( "field2", Integer.class );
        final OperatorRuntimeSchemaBuilder operatorRuntimeSchemaBuilder2 = new OperatorRuntimeSchemaBuilder( 2, 1 );
        operatorRuntimeSchemaBuilder2.getInputPortSchemaBuilder( 0 ).addField( "field2", Integer.class );
        operatorRuntimeSchemaBuilder2.getInputPortSchemaBuilder( 1 ).addField( "field2", Integer.class );
        operatorRuntimeSchemaBuilder2.getOutputPortSchemaBuilder( 0 ).addField( "field3", Integer.class );
        final OperatorRuntimeSchemaBuilder operatorRuntimeSchemaBuilder3 = new OperatorRuntimeSchemaBuilder( 1, 1 );
        operatorRuntimeSchemaBuilder3.getInputPortSchemaBuilder( 0 ).addField( "field3", Integer.class );
        operatorRuntimeSchemaBuilder3.getOutputPortSchemaBuilder( 0 ).addField( "field3", Integer.class );
        final OperatorDefinition operatorDefinition0 = OperatorDefinitionBuilder.newInstance( "op0",
                                                                                              StatefulOperatorWithSingleInputOutputPortCount.class )
                                                                                .setExtendingSchema( operatorRuntimeSchemaBuilder0 )
                                                                                .build();
        final OperatorDefinition operatorDefinition1 = OperatorDefinitionBuilder.newInstance( "op1",
                                                                                              StatelessOperatorWithSingleInputOutputPortCount.class )
                                                                                .setExtendingSchema( operatorRuntimeSchemaBuilder1 )
                                                                                .build();
        final OperatorDefinition operatorDefinition2 = OperatorDefinitionBuilder.newInstance( "op2",
                                                                                              PartitionedStatefulOperatorWithSingleInputOutputPortCount.class )
                                                                                .setExtendingSchema( operatorRuntimeSchemaBuilder2 )
                                                                                .setPartitionFieldNames( singletonList( "field2" ) )
                                                                                .build();
        final OperatorDefinition operatorDefinition3 = OperatorDefinitionBuilder.newInstance( "op3",
                                                                                              StatelessOperatorWithSingleInputOutputPortCount.class )
                                                                                .setExtendingSchema( operatorRuntimeSchemaBuilder3 )
                                                                                .build();

        final FlowDefinition flow = new FlowDefinitionBuilder().add( operatorDefinition0 )
                                                               .add( operatorDefinition1 )
                                                               .add( operatorDefinition2 )
                                                               .add( operatorDefinition3 )
                                                               .connect( "op0", "op1" )
                                                               .connect( "op1", "op2" )
                                                               .connect( "op2", "op3" )
                                                               .build();

        final List<RegionDefinition> regions = new RegionFormerImpl().createRegions( flow );
        final RegionDefinition region = regions.get( 1 );
        final RegionRuntimeConfig regionConfig = new RegionRuntimeConfig( 1, region, 1, asList( 0, 1 ) );

        final RegionInstance regionInstance = regionManager.createRegion( flow, regionConfig );
        assertNotNull( regionInstance );
        final PipelineInstance[] pipelines = regionInstance.getPipelines( 0 );
        assertEquals( 2, pipelines.length );
        final PipelineInstance pipeline0 = pipelines[ 0 ];
        final PipelineInstance pipeline1 = pipelines[ 1 ];
        assertEmptyTupleQueueContext( pipeline0, operatorDefinition1.inputPortCount() );
        assertDefaultTupleQueueContext( pipeline1, operatorDefinition2.inputPortCount() );

        assertEquals( new PipelineInstanceId( 1, 0, 0 ), pipeline0.id() );
        assertEquals( new PipelineInstanceId( 1, 0, 1 ), pipeline1.id() );
        assertEquals( 1, pipeline0.getOperatorCount() );
        assertEquals( 2, pipeline1.getOperatorCount() );

        final OperatorInstance operatorInstance1 = pipeline0.getOperatorInstance( 0 );
        assertOperatorDefinition( operatorInstance1, operatorDefinition1 );
        assertDefaultTupleQueueContext( operatorInstance1, operatorDefinition1.inputPortCount(), MULTI_THREADED );
        assertEmptyKVStoreContext( operatorInstance1 );
        assertBlockingTupleQueueDrainerPool( operatorInstance1 );
        assertNonCachedTuplesImplSupplier( operatorInstance1 );

        final OperatorInstance operatorInstance2 = pipeline1.getOperatorInstance( 0 );
        assertOperatorDefinition( operatorInstance2, operatorDefinition2 );
        assertPartitionedTupleQueueContext( operatorInstance2 );
        assertPartitionedKVStoreContext( operatorInstance2 );
        assertBlockingTupleQueueDrainerPool( operatorInstance2 );
        assertCachedTuplesImplSupplier( operatorInstance2 );

        final OperatorInstance operatorInstance3 = pipeline1.getOperatorInstance( 1 );
        assertOperatorDefinition( operatorInstance3, operatorDefinition3 );
        assertDefaultTupleQueueContext( operatorInstance3, operatorDefinition3.inputPortCount(), SINGLE_THREADED );
        assertEmptyKVStoreContext( operatorInstance3 );
        assertNonBlockingTupleQueueDrainerPool( operatorInstance3 );
        assertNonCachedTuplesImplSupplier( operatorInstance3 );
    }

    private void assertEmptyTupleQueueContext ( final PipelineInstance pipeline, final int inputPortCount )
    {
        final TupleQueueContext tupleQueueContext = pipeline.getUpstreamTupleQueueContext();
        assertTrue( tupleQueueContext instanceof EmptyTupleQueueContext );
        assertEquals( inputPortCount, ( (EmptyTupleQueueContext) tupleQueueContext ).getInputPortCount() );
    }

    private void assertDefaultTupleQueueContext ( final PipelineInstance pipeline, final int inputPortCount )
    {
        final TupleQueueContext tupleQueueContext = pipeline.getUpstreamTupleQueueContext();
        assertTrue( tupleQueueContext instanceof DefaultTupleQueueContext );
        assertEquals( MULTI_THREADED, ( (DefaultTupleQueueContext) tupleQueueContext ).getThreadingPreference() );
        assertEquals( inputPortCount, ( (DefaultTupleQueueContext) tupleQueueContext ).getInputPortCount() );
    }

    private void assertOperatorDefinition ( final OperatorInstance operatorInstance, final OperatorDefinition operatorDefinition )
    {
        assertTrue( operatorInstance.getOperatorDefinition() == operatorDefinition );
    }

    private void assertEmptyTupleQueueContext ( final OperatorInstance operatorInstance )
    {
        final TupleQueueContext tupleQueueContext = operatorInstance.getQueue();
        assertTrue( tupleQueueContext instanceof EmptyTupleQueueContext );
        assertEquals( operatorInstance.getOperatorDefinition().id(), tupleQueueContext.getOperatorId() );
    }

    private void assertDefaultTupleQueueContext ( final OperatorInstance operatorInstance,
                                                  final int inputPortCount,
                                                  final ThreadingPreference threadingPreference )
    {
        final TupleQueueContext tupleQueueContext = operatorInstance.getQueue();
        assertTrue( tupleQueueContext instanceof DefaultTupleQueueContext );
        assertEquals( operatorInstance.getOperatorDefinition().id(), tupleQueueContext.getOperatorId() );
        assertEquals( threadingPreference, ( (DefaultTupleQueueContext) tupleQueueContext ).getThreadingPreference() );
        assertEquals( inputPortCount, ( (DefaultTupleQueueContext) tupleQueueContext ).getInputPortCount() );
    }

    private void assertPartitionedTupleQueueContext ( final OperatorInstance operatorInstance )
    {
        final TupleQueueContext tupleQueueContext = operatorInstance.getQueue();
        assertTrue( tupleQueueContext instanceof PartitionedTupleQueueContext );
        assertEquals( operatorInstance.getOperatorDefinition().id(), tupleQueueContext.getOperatorId() );
    }

    private void assertEmptyKVStoreContext ( final OperatorInstance operatorInstance )
    {
        assertTrue( operatorInstance.getKvStoreContext() instanceof EmptyKVStoreContext );
    }

    private void assertDefaultKVStoreContext ( final OperatorInstance operatorInstance )
    {
        final KVStoreContext kvStoreContext = operatorInstance.getKvStoreContext();
        assertTrue( kvStoreContext instanceof DefaultKVStoreContext );
        assertEquals( operatorInstance.getOperatorDefinition().id(), kvStoreContext.getOperatorId() );
    }

    private void assertPartitionedKVStoreContext ( final OperatorInstance operatorInstance )
    {
        final KVStoreContext kvStoreContext = operatorInstance.getKvStoreContext();
        assertTrue( kvStoreContext instanceof PartitionedKVStoreContext );
        assertEquals( operatorInstance.getOperatorDefinition().id(), kvStoreContext.getOperatorId() );
    }

    private void assertBlockingTupleQueueDrainerPool ( final OperatorInstance operatorInstance )
    {
        assertTrue( operatorInstance.getDrainerPool() instanceof BlockingTupleQueueDrainerPool );
    }

    private void assertNonBlockingTupleQueueDrainerPool ( final OperatorInstance operatorInstance )
    {
        assertTrue( operatorInstance.getDrainerPool() instanceof NonBlockingTupleQueueDrainerPool );
    }

    private void assertCachedTuplesImplSupplier ( final OperatorInstance operatorInstance )
    {
        assertTrue( operatorInstance.getOutputSupplier() instanceof CachedTuplesImplSupplier );
    }

    private void assertNonCachedTuplesImplSupplier ( final OperatorInstance operatorInstance )
    {
        assertTrue( operatorInstance.getOutputSupplier() instanceof NonCachedTuplesImplSupplier );
    }

    @OperatorSpec( type = OperatorType.STATELESS, inputPortCount = 1, outputPortCount = 1 )
    private static class StatelessOperatorWithSingleInputOutputPortCount extends NopOperator
    {

    }


    @OperatorSpec( type = OperatorType.PARTITIONED_STATEFUL, inputPortCount = 2, outputPortCount = 1 )
    private static class PartitionedStatefulOperatorWithSingleInputOutputPortCount extends NopOperator
    {

    }


    @OperatorSpec( type = OperatorType.STATEFUL, inputPortCount = 3, outputPortCount = 1 )
    private static class StatefulOperatorWithSingleInputOutputPortCount extends NopOperator
    {

    }


    static class NopOperator implements Operator
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

}
