package cs.bilkent.zanza.engine.region.impl;

import java.util.List;

import org.junit.Before;
import org.junit.Ignore;
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
import cs.bilkent.zanza.engine.partition.impl.PartitionKeyFunctionFactoryImpl;
import cs.bilkent.zanza.engine.pipeline.OperatorReplica;
import cs.bilkent.zanza.engine.pipeline.PipelineId;
import cs.bilkent.zanza.engine.pipeline.PipelineReplica;
import cs.bilkent.zanza.engine.pipeline.PipelineReplicaId;
import cs.bilkent.zanza.engine.pipeline.impl.tuplesupplier.CachedTuplesImplSupplier;
import cs.bilkent.zanza.engine.pipeline.impl.tuplesupplier.NonCachedTuplesImplSupplier;
import cs.bilkent.zanza.engine.region.Region;
import cs.bilkent.zanza.engine.region.RegionDefinition;
import cs.bilkent.zanza.engine.region.RegionRuntimeConfig;
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

public class RegionManagerImplTest
{

    private RegionManagerImpl regionManagerImpl;

    @Before
    public void init ()
    {
        final ZanzaConfig config = new ZanzaConfig();
        final PartitionService partitionService = new PartitionServiceImpl( config );
        final KVStoreContextManagerImpl kvStoreContextManager = new KVStoreContextManagerImpl( partitionService );
        final TupleQueueContextManagerImpl tupleQueueContextManager = new TupleQueueContextManagerImpl( config,
                                                                                                        partitionService,
                                                                                                        new PartitionKeyFunctionFactoryImpl() );
        regionManagerImpl = new RegionManagerImpl( config, kvStoreContextManager, tupleQueueContextManager );
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

        final List<RegionDefinition> regionDefs = new RegionDefinitionFormerImpl().createRegions( flow );
        final RegionDefinition regionDef = regionDefs.get( 0 );
        final RegionRuntimeConfig regionConfig = new RegionRuntimeConfig( 1, regionDef, 1, singletonList( 0 ) );

        final Region region = regionManagerImpl.createRegion( flow, regionConfig );
        assertNotNull( region );
        final PipelineReplica[] pipelines = region.getReplicaPipelines( 0 );
        assertEquals( 1, pipelines.length );
        final PipelineReplica pipeline = pipelines[ 0 ];
        assertEmptyTupleQueueContext( pipeline, operatorDefinition0.inputPortCount() );

        assertStatelessPipelineWithNoInput( 1, 0, 0, operatorDefinition0, operatorDefinition1, pipeline );
    }

    // TODO fix it after splitters and mergers are implemented
    @Ignore
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

        final List<RegionDefinition> regionDefs = new RegionDefinitionFormerImpl().createRegions( flow );
        final RegionDefinition regionDef = regionDefs.get( 0 );
        final RegionRuntimeConfig regionConfig = new RegionRuntimeConfig( 1, regionDef, 2, singletonList( 0 ) );

        final Region region = regionManagerImpl.createRegion( flow, regionConfig );
        assertNotNull( region );
        final PipelineReplica[] pipelines0 = region.getReplicaPipelines( 0 );
        final PipelineReplica[] pipelines1 = region.getReplicaPipelines( 1 );
        assertEquals( 1, pipelines0.length );
        final PipelineReplica pipelineReplica0 = pipelines0[ 0 ];
        final PipelineReplica pipelineReplica1 = pipelines1[ 0 ];
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

        final List<RegionDefinition> regionDefs = new RegionDefinitionFormerImpl().createRegions( flow );
        final RegionDefinition regionDef = regionDefs.get( 0 );
        final RegionRuntimeConfig regionConfig = new RegionRuntimeConfig( 1, regionDef, 1, asList( 0, 1 ) );

        final Region region = regionManagerImpl.createRegion( flow, regionConfig );
        assertNotNull( region );
        final PipelineReplica[] pipelines = region.getReplicaPipelines( 0 );
        assertEquals( 2, pipelines.length );
        final PipelineReplica pipeline0 = pipelines[ 0 ];
        final PipelineReplica pipeline1 = pipelines[ 1 ];
        assertEmptyTupleQueueContext( pipeline0, operatorDefinition0.inputPortCount() );
        assertDefaultTupleQueueContext( pipeline1, operatorDefinition1.inputPortCount() );

        assertEquals( new PipelineReplicaId( new PipelineId( 1, 0 ), 0 ), pipeline0.id() );
        assertEquals( new PipelineReplicaId( new PipelineId( 1, 1 ), 0 ), pipeline1.id() );
        assertEquals( 1, pipeline0.getOperatorCount() );
        assertEquals( 1, pipeline1.getOperatorCount() );
        final OperatorReplica operatorReplica0 = pipeline0.getOperatorInstance( 0 );
        final OperatorReplica operatorReplica1 = pipeline1.getOperatorInstance( 0 );
        assertOperatorDefinition( operatorReplica0, operatorDefinition0 );
        assertOperatorDefinition( operatorReplica1, operatorDefinition1 );
        assertEmptyTupleQueueContext( operatorReplica0 );
        assertDefaultTupleQueueContext( operatorReplica1, operatorDefinition1.inputPortCount(), MULTI_THREADED );
        assertEmptyKVStoreContext( operatorReplica0 );
        assertEmptyKVStoreContext( operatorReplica1 );
        assertBlockingTupleQueueDrainerPool( operatorReplica0 );
        assertBlockingTupleQueueDrainerPool( operatorReplica1 );
        assertNonCachedTuplesImplSupplier( operatorReplica0 );
        assertNonCachedTuplesImplSupplier( operatorReplica1 );
    }

    private void assertStatelessPipelineWithNoInput ( final int regionId,
                                                      final int replicaIndex,
                                                      final int pipelineId,
                                                      final OperatorDefinition operatorDefinition0,
                                                      final OperatorDefinition operatorDefinition1, final PipelineReplica pipelineReplica0 )
    {
        assertEquals( new PipelineReplicaId( new PipelineId( regionId, pipelineId ), replicaIndex ), pipelineReplica0.id() );
        assertEquals( 2, pipelineReplica0.getOperatorCount() );
        final OperatorReplica operatorReplica0 = pipelineReplica0.getOperatorInstance( 0 );
        final OperatorReplica operatorReplica1 = pipelineReplica0.getOperatorInstance( 1 );
        assertOperatorDefinition( operatorReplica0, operatorDefinition0 );
        assertOperatorDefinition( operatorReplica1, operatorDefinition1 );
        assertEmptyTupleQueueContext( operatorReplica0 );
        assertDefaultTupleQueueContext( operatorReplica1, operatorDefinition1.inputPortCount(), SINGLE_THREADED );
        assertEmptyKVStoreContext( operatorReplica0 );
        assertEmptyKVStoreContext( operatorReplica1 );
        assertBlockingTupleQueueDrainerPool( operatorReplica0 );
        assertNonBlockingTupleQueueDrainerPool( operatorReplica1 );
        assertCachedTuplesImplSupplier( operatorReplica0 );
        assertNonCachedTuplesImplSupplier( operatorReplica1 );
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

        final List<RegionDefinition> regionDefs = new RegionDefinitionFormerImpl().createRegions( flow );
        final RegionDefinition regionDef = regionDefs.get( 1 );
        final RegionRuntimeConfig regionConfig = new RegionRuntimeConfig( 1, regionDef, 1, singletonList( 0 ) );

        final Region regionInstance = regionManagerImpl.createRegion( flow, regionConfig );
        assertNotNull( regionInstance );
        final PipelineReplica[] pipelines = regionInstance.getReplicaPipelines( 0 );
        assertEquals( 1, pipelines.length );
        final PipelineReplica pipeline = pipelines[ 0 ];
        assertDefaultTupleQueueContext( pipeline, operatorDefinition1.inputPortCount() );

        assertEquals( new PipelineReplicaId( new PipelineId( 1, 0 ), 0 ), pipeline.id() );
        assertEquals( 2, pipeline.getOperatorCount() );
        final OperatorReplica operatorReplica1 = pipeline.getOperatorInstance( 0 );
        final OperatorReplica operatorReplica2 = pipeline.getOperatorInstance( 1 );
        assertOperatorDefinition( operatorReplica1, operatorDefinition1 );
        assertOperatorDefinition( operatorReplica2, operatorDefinition2 );
        assertDefaultTupleQueueContext( operatorReplica1, operatorDefinition1.inputPortCount(), MULTI_THREADED );
        assertDefaultTupleQueueContext( operatorReplica2, operatorDefinition2.inputPortCount(), SINGLE_THREADED );
        assertEmptyKVStoreContext( operatorReplica1 );
        assertEmptyKVStoreContext( operatorReplica2 );
        assertBlockingTupleQueueDrainerPool( operatorReplica1 );
        assertNonBlockingTupleQueueDrainerPool( operatorReplica2 );
        assertCachedTuplesImplSupplier( operatorReplica1 );
        assertNonCachedTuplesImplSupplier( operatorReplica2 );
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

        final List<RegionDefinition> regionDefs = new RegionDefinitionFormerImpl().createRegions( flow );
        final RegionDefinition regionDef = regionDefs.get( 1 );
        final RegionRuntimeConfig regionConfig = new RegionRuntimeConfig( 1, regionDef, 1, singletonList( 0 ) );

        final Region regionInstance = regionManagerImpl.createRegion( flow, regionConfig );
        assertNotNull( regionInstance );
        final PipelineReplica[] pipelines = regionInstance.getReplicaPipelines( 0 );
        assertEquals( 1, pipelines.length );
        final PipelineReplica pipeline = pipelines[ 0 ];
        assertDefaultTupleQueueContext( pipeline, operatorDefinition1.inputPortCount() );

        assertEquals( new PipelineReplicaId( new PipelineId( 1, 0 ), 0 ), pipeline.id() );
        assertEquals( 1, pipeline.getOperatorCount() );

        final OperatorReplica operatorReplica1 = pipeline.getOperatorInstance( 0 );
        assertOperatorDefinition( operatorReplica1, operatorDefinition1 );
        assertDefaultTupleQueueContext( operatorReplica1, operatorDefinition1.inputPortCount(), MULTI_THREADED );
        assertDefaultKVStoreContext( operatorReplica1 );
        assertBlockingTupleQueueDrainerPool( operatorReplica1 );
        assertNonCachedTuplesImplSupplier( operatorReplica1 );
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

        final List<RegionDefinition> regionDefs = new RegionDefinitionFormerImpl().createRegions( flow );
        final RegionDefinition regionDef = regionDefs.get( 1 );
        final RegionRuntimeConfig regionConfig = new RegionRuntimeConfig( 1, regionDef, 1, singletonList( 0 ) );

        final Region regionInstance = regionManagerImpl.createRegion( flow, regionConfig );
        assertNotNull( regionInstance );
        final PipelineReplica[] pipelines = regionInstance.getReplicaPipelines( 0 );
        assertEquals( 1, pipelines.length );
        final PipelineReplica pipeline = pipelines[ 0 ];
        assertDefaultTupleQueueContext( pipeline, operatorDefinition1.inputPortCount() );

        assertEquals( new PipelineReplicaId( new PipelineId( 1, 0 ), 0 ), pipeline.id() );
        assertEquals( 1, pipeline.getOperatorCount() );

        final OperatorReplica operatorReplica1 = pipeline.getOperatorInstance( 0 );
        assertOperatorDefinition( operatorReplica1, operatorDefinition1 );
        assertPartitionedTupleQueueContext( operatorReplica1 );
        assertPartitionedKVStoreContext( operatorReplica1 );
        assertBlockingTupleQueueDrainerPool( operatorReplica1 );
        assertNonCachedTuplesImplSupplier( operatorReplica1 );
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

        final List<RegionDefinition> regionDefs = new RegionDefinitionFormerImpl().createRegions( flow );
        final RegionDefinition regionDef = regionDefs.get( 1 );
        final RegionRuntimeConfig regionConfig = new RegionRuntimeConfig( 1, regionDef, 2, singletonList( 0 ) );

        final Region regionInstance = regionManagerImpl.createRegion( flow, regionConfig );
        assertNotNull( regionInstance );
        final PipelineReplica[] pipelines0 = regionInstance.getReplicaPipelines( 0 );
        final PipelineReplica[] pipelines1 = regionInstance.getReplicaPipelines( 1 );
        assertEquals( 1, pipelines0.length );
        assertEquals( 1, pipelines1.length );
        final PipelineReplica pipelineReplica0 = pipelines0[ 0 ];
        final PipelineReplica pipelineReplica1 = pipelines1[ 0 ];
        assertDefaultTupleQueueContext( pipelineReplica0, operatorDefinition1.inputPortCount() );
        assertDefaultTupleQueueContext( pipelineReplica1, operatorDefinition1.inputPortCount() );

        assertEquals( new PipelineReplicaId( new PipelineId( 1, 0 ), 0 ), pipelineReplica0.id() );
        assertEquals( new PipelineReplicaId( new PipelineId( 1, 0 ), 1 ), pipelineReplica1.id() );
        assertEquals( 1, pipelineReplica0.getOperatorCount() );
        assertEquals( 1, pipelineReplica1.getOperatorCount() );

        final OperatorReplica operatorReplica0 = pipelineReplica0.getOperatorInstance( 0 );
        final OperatorReplica operatorReplica1 = pipelineReplica1.getOperatorInstance( 0 );
        assertOperatorDefinition( operatorReplica0, operatorDefinition1 );
        assertOperatorDefinition( operatorReplica1, operatorDefinition1 );
        assertPartitionedTupleQueueContext( operatorReplica0 );
        assertPartitionedTupleQueueContext( operatorReplica1 );
        assertPartitionedKVStoreContext( operatorReplica0 );
        assertPartitionedKVStoreContext( operatorReplica1 );
        assertBlockingTupleQueueDrainerPool( operatorReplica0 );
        assertBlockingTupleQueueDrainerPool( operatorReplica1 );
        assertNonCachedTuplesImplSupplier( operatorReplica0 );
        assertNonCachedTuplesImplSupplier( operatorReplica1 );
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

        final List<RegionDefinition> regionDefs = new RegionDefinitionFormerImpl().createRegions( flow );
        final RegionDefinition regionDef = regionDefs.get( 1 );
        final RegionRuntimeConfig regionConfig = new RegionRuntimeConfig( 1, regionDef, 1, singletonList( 0 ) );

        final Region region = regionManagerImpl.createRegion( flow, regionConfig );
        assertNotNull( region );
        final PipelineReplica[] pipelines = region.getReplicaPipelines( 0 );
        assertEquals( 1, pipelines.length );
        final PipelineReplica pipeline = pipelines[ 0 ];
        assertDefaultTupleQueueContext( pipeline, operatorDefinition1.inputPortCount() );

        assertEquals( new PipelineReplicaId( new PipelineId( 1, 0 ), 0 ), pipeline.id() );
        assertEquals( 2, pipeline.getOperatorCount() );

        final OperatorReplica operatorReplica1 = pipeline.getOperatorInstance( 0 );
        assertOperatorDefinition( operatorReplica1, operatorDefinition1 );
        assertDefaultTupleQueueContext( operatorReplica1, operatorDefinition1.inputPortCount(), MULTI_THREADED );
        assertEmptyKVStoreContext( operatorReplica1 );
        assertBlockingTupleQueueDrainerPool( operatorReplica1 );
        assertCachedTuplesImplSupplier( operatorReplica1 );

        final OperatorReplica operatorReplica2 = pipeline.getOperatorInstance( 1 );
        assertOperatorDefinition( operatorReplica2, operatorDefinition2 );
        assertPartitionedTupleQueueContext( operatorReplica2 );
        assertPartitionedKVStoreContext( operatorReplica2 );
        assertNonBlockingTupleQueueDrainerPool( operatorReplica2 );
        assertNonCachedTuplesImplSupplier( operatorReplica2 );
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

        final List<RegionDefinition> regionDefs = new RegionDefinitionFormerImpl().createRegions( flow );
        final RegionDefinition regionDef = regionDefs.get( 1 );
        final RegionRuntimeConfig regionConfig = new RegionRuntimeConfig( 1, regionDef, 2, singletonList( 0 ) );

        final Region region = regionManagerImpl.createRegion( flow, regionConfig );
        assertNotNull( region );
        final PipelineReplica[] pipelinesReplica0 = region.getReplicaPipelines( 0 );
        final PipelineReplica[] pipelinesReplica1 = region.getReplicaPipelines( 1 );
        assertEquals( 1, pipelinesReplica0.length );
        assertEquals( 1, pipelinesReplica1.length );
        final PipelineReplica pipelineReplica0 = pipelinesReplica0[ 0 ];
        final PipelineReplica pipelineReplica1 = pipelinesReplica1[ 0 ];
        assertDefaultTupleQueueContext( pipelineReplica0, operatorDefinition1.inputPortCount() );
        assertDefaultTupleQueueContext( pipelineReplica1, operatorDefinition1.inputPortCount() );

        assertEquals( new PipelineReplicaId( new PipelineId( 1, 0 ), 0 ), pipelineReplica0.id() );
        assertEquals( new PipelineReplicaId( new PipelineId( 1, 0 ), 1 ), pipelineReplica1.id() );
        assertEquals( 2, pipelineReplica0.getOperatorCount() );
        assertEquals( 2, pipelineReplica1.getOperatorCount() );

        final OperatorReplica operatorReplica0_1 = pipelineReplica0.getOperatorInstance( 0 );
        assertOperatorDefinition( operatorReplica0_1, operatorDefinition1 );
        assertDefaultTupleQueueContext( operatorReplica0_1, operatorDefinition1.inputPortCount(), MULTI_THREADED );
        assertEmptyKVStoreContext( operatorReplica0_1 );
        assertBlockingTupleQueueDrainerPool( operatorReplica0_1 );
        assertCachedTuplesImplSupplier( operatorReplica0_1 );

        final OperatorReplica operatorReplica1_1 = pipelineReplica0.getOperatorInstance( 0 );
        assertOperatorDefinition( operatorReplica1_1, operatorDefinition1 );
        assertDefaultTupleQueueContext( operatorReplica1_1, operatorDefinition1.inputPortCount(), MULTI_THREADED );
        assertEmptyKVStoreContext( operatorReplica1_1 );
        assertBlockingTupleQueueDrainerPool( operatorReplica1_1 );
        assertCachedTuplesImplSupplier( operatorReplica1_1 );

        final OperatorReplica operatorReplica0_2 = pipelineReplica0.getOperatorInstance( 1 );
        assertOperatorDefinition( operatorReplica0_2, operatorDefinition2 );
        assertPartitionedTupleQueueContext( operatorReplica0_2 );
        assertPartitionedKVStoreContext( operatorReplica0_2 );
        assertNonBlockingTupleQueueDrainerPool( operatorReplica0_2 );
        assertNonCachedTuplesImplSupplier( operatorReplica0_2 );

        final OperatorReplica operatorReplica1_2 = pipelineReplica0.getOperatorInstance( 1 );
        assertOperatorDefinition( operatorReplica1_2, operatorDefinition2 );
        assertPartitionedTupleQueueContext( operatorReplica1_2 );
        assertPartitionedKVStoreContext( operatorReplica1_2 );
        assertNonBlockingTupleQueueDrainerPool( operatorReplica1_2 );
        assertNonCachedTuplesImplSupplier( operatorReplica1_2 );
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

        final List<RegionDefinition> regionDefs = new RegionDefinitionFormerImpl().createRegions( flow );
        final RegionDefinition regionDef = regionDefs.get( 1 );
        final RegionRuntimeConfig regionConfig = new RegionRuntimeConfig( 1, regionDef, 1, asList( 0, 1 ) );

        final Region region = regionManagerImpl.createRegion( flow, regionConfig );
        assertNotNull( region );
        final PipelineReplica[] pipelines = region.getReplicaPipelines( 0 );
        assertEquals( 2, pipelines.length );
        final PipelineReplica pipeline0 = pipelines[ 0 ];
        final PipelineReplica pipeline1 = pipelines[ 1 ];
        assertDefaultTupleQueueContext( pipeline0, operatorDefinition1.inputPortCount() );
        assertDefaultTupleQueueContext( pipeline1, operatorDefinition2.inputPortCount() );

        assertEquals( new PipelineReplicaId( new PipelineId( 1, 0 ), 0 ), pipeline0.id() );
        assertEquals( new PipelineReplicaId( new PipelineId( 1, 1 ), 0 ), pipeline1.id() );
        assertEquals( 1, pipeline0.getOperatorCount() );
        assertEquals( 2, pipeline1.getOperatorCount() );

        final OperatorReplica operatorReplica1 = pipeline0.getOperatorInstance( 0 );
        assertOperatorDefinition( operatorReplica1, operatorDefinition1 );
        assertDefaultTupleQueueContext( operatorReplica1, operatorDefinition1.inputPortCount(), MULTI_THREADED );
        assertEmptyKVStoreContext( operatorReplica1 );
        assertBlockingTupleQueueDrainerPool( operatorReplica1 );
        assertNonCachedTuplesImplSupplier( operatorReplica1 );

        final OperatorReplica operatorReplica2 = pipeline1.getOperatorInstance( 0 );
        assertOperatorDefinition( operatorReplica2, operatorDefinition2 );
        assertPartitionedTupleQueueContext( operatorReplica2 );
        assertPartitionedKVStoreContext( operatorReplica2 );
        assertBlockingTupleQueueDrainerPool( operatorReplica2 );
        assertCachedTuplesImplSupplier( operatorReplica2 );

        final OperatorReplica operatorReplica3 = pipeline1.getOperatorInstance( 1 );
        assertOperatorDefinition( operatorReplica3, operatorDefinition3 );
        assertDefaultTupleQueueContext( operatorReplica3, operatorDefinition3.inputPortCount(), SINGLE_THREADED );
        assertEmptyKVStoreContext( operatorReplica3 );
        assertNonBlockingTupleQueueDrainerPool( operatorReplica3 );
        assertNonCachedTuplesImplSupplier( operatorReplica3 );
    }

    private void assertEmptyTupleQueueContext ( final PipelineReplica pipeline, final int inputPortCount )
    {
        final TupleQueueContext tupleQueueContext = pipeline.getUpstreamTupleQueueContext();
        assertTrue( tupleQueueContext instanceof EmptyTupleQueueContext );
        assertEquals( inputPortCount, tupleQueueContext.getInputPortCount() );
    }

    private void assertDefaultTupleQueueContext ( final PipelineReplica pipeline, final int inputPortCount )
    {
        final TupleQueueContext tupleQueueContext = pipeline.getUpstreamTupleQueueContext();
        assertTrue( tupleQueueContext instanceof DefaultTupleQueueContext );
        assertEquals( MULTI_THREADED, ( (DefaultTupleQueueContext) tupleQueueContext ).getThreadingPreference() );
        assertEquals( inputPortCount, tupleQueueContext.getInputPortCount() );
    }

    private void assertOperatorDefinition ( final OperatorReplica operatorReplica, final OperatorDefinition operatorDefinition )
    {
        assertTrue( operatorReplica.getOperatorDefinition() == operatorDefinition );
    }

    private void assertEmptyTupleQueueContext ( final OperatorReplica operatorReplica )
    {
        final TupleQueueContext tupleQueueContext = operatorReplica.getQueue();
        assertTrue( tupleQueueContext instanceof EmptyTupleQueueContext );
        assertEquals( operatorReplica.getOperatorDefinition().id(), tupleQueueContext.getOperatorId() );
    }

    private void assertDefaultTupleQueueContext ( final OperatorReplica operatorReplica,
                                                  final int inputPortCount,
                                                  final ThreadingPreference threadingPreference )
    {
        final TupleQueueContext tupleQueueContext = operatorReplica.getQueue();
        assertTrue( tupleQueueContext instanceof DefaultTupleQueueContext );
        assertEquals( operatorReplica.getOperatorDefinition().id(), tupleQueueContext.getOperatorId() );
        assertEquals( threadingPreference, ( (DefaultTupleQueueContext) tupleQueueContext ).getThreadingPreference() );
        assertEquals( inputPortCount, tupleQueueContext.getInputPortCount() );
    }

    private void assertPartitionedTupleQueueContext ( final OperatorReplica operatorReplica )
    {
        final TupleQueueContext tupleQueueContext = operatorReplica.getQueue();
        assertTrue( tupleQueueContext instanceof PartitionedTupleQueueContext );
        assertEquals( operatorReplica.getOperatorDefinition().id(), tupleQueueContext.getOperatorId() );
    }

    private void assertEmptyKVStoreContext ( final OperatorReplica operatorReplica )
    {
        assertTrue( operatorReplica.getKvStoreContext() instanceof EmptyKVStoreContext );
    }

    private void assertDefaultKVStoreContext ( final OperatorReplica operatorReplica )
    {
        final KVStoreContext kvStoreContext = operatorReplica.getKvStoreContext();
        assertTrue( kvStoreContext instanceof DefaultKVStoreContext );
        assertEquals( operatorReplica.getOperatorDefinition().id(), kvStoreContext.getOperatorId() );
    }

    private void assertPartitionedKVStoreContext ( final OperatorReplica operatorReplica )
    {
        final KVStoreContext kvStoreContext = operatorReplica.getKvStoreContext();
        assertTrue( kvStoreContext instanceof PartitionedKVStoreContext );
        assertEquals( operatorReplica.getOperatorDefinition().id(), kvStoreContext.getOperatorId() );
    }

    private void assertBlockingTupleQueueDrainerPool ( final OperatorReplica operatorReplica )
    {
        assertTrue( operatorReplica.getDrainerPool() instanceof BlockingTupleQueueDrainerPool );
    }

    private void assertNonBlockingTupleQueueDrainerPool ( final OperatorReplica operatorReplica )
    {
        assertTrue( operatorReplica.getDrainerPool() instanceof NonBlockingTupleQueueDrainerPool );
    }

    private void assertCachedTuplesImplSupplier ( final OperatorReplica operatorReplica )
    {
        assertTrue( operatorReplica.getOutputSupplier() instanceof CachedTuplesImplSupplier );
    }

    private void assertNonCachedTuplesImplSupplier ( final OperatorReplica operatorReplica )
    {
        assertTrue( operatorReplica.getOutputSupplier() instanceof NonCachedTuplesImplSupplier );
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
