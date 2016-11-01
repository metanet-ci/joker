package cs.bilkent.joker.engine.region.impl;

import java.util.List;

import org.junit.Test;

import cs.bilkent.joker.engine.config.JokerConfig;
import cs.bilkent.joker.engine.config.ThreadingPreference;
import static cs.bilkent.joker.engine.config.ThreadingPreference.MULTI_THREADED;
import static cs.bilkent.joker.engine.config.ThreadingPreference.SINGLE_THREADED;
import cs.bilkent.joker.engine.kvstore.OperatorKVStore;
import cs.bilkent.joker.engine.kvstore.impl.DefaultOperatorKVStore;
import cs.bilkent.joker.engine.kvstore.impl.EmptyOperatorKVStore;
import cs.bilkent.joker.engine.kvstore.impl.OperatorKVStoreManagerImpl;
import cs.bilkent.joker.engine.kvstore.impl.PartitionedOperatorKVStore;
import cs.bilkent.joker.engine.partition.PartitionService;
import cs.bilkent.joker.engine.partition.PartitionServiceImpl;
import cs.bilkent.joker.engine.partition.impl.PartitionKeyExtractorFactoryImpl;
import cs.bilkent.joker.engine.pipeline.OperatorReplica;
import cs.bilkent.joker.engine.pipeline.PipelineId;
import cs.bilkent.joker.engine.pipeline.PipelineReplica;
import cs.bilkent.joker.engine.pipeline.PipelineReplicaId;
import cs.bilkent.joker.engine.pipeline.impl.tuplesupplier.CachedTuplesImplSupplier;
import cs.bilkent.joker.engine.pipeline.impl.tuplesupplier.NonCachedTuplesImplSupplier;
import cs.bilkent.joker.engine.region.FlowDeploymentDef;
import cs.bilkent.joker.engine.region.PipelineTransformer;
import cs.bilkent.joker.engine.region.Region;
import cs.bilkent.joker.engine.region.RegionConfig;
import cs.bilkent.joker.engine.region.RegionDef;
import cs.bilkent.joker.engine.tuplequeue.OperatorTupleQueue;
import cs.bilkent.joker.engine.tuplequeue.impl.OperatorTupleQueueManagerImpl;
import cs.bilkent.joker.engine.tuplequeue.impl.drainer.pool.BlockingTupleQueueDrainerPool;
import cs.bilkent.joker.engine.tuplequeue.impl.drainer.pool.NonBlockingTupleQueueDrainerPool;
import cs.bilkent.joker.engine.tuplequeue.impl.operator.DefaultOperatorTupleQueue;
import cs.bilkent.joker.engine.tuplequeue.impl.operator.EmptyOperatorTupleQueue;
import cs.bilkent.joker.engine.tuplequeue.impl.operator.PartitionedOperatorTupleQueue;
import cs.bilkent.joker.flow.FlowDef;
import cs.bilkent.joker.flow.FlowDefBuilder;
import cs.bilkent.joker.operator.InitializationContext;
import cs.bilkent.joker.operator.InvocationContext;
import cs.bilkent.joker.operator.Operator;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.OperatorDefBuilder;
import cs.bilkent.joker.operator.scheduling.SchedulingStrategy;
import cs.bilkent.joker.operator.schema.runtime.OperatorRuntimeSchemaBuilder;
import cs.bilkent.joker.operator.spec.OperatorSpec;
import cs.bilkent.joker.operator.spec.OperatorType;
import cs.bilkent.joker.testutils.AbstractJokerTest;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class RegionManagerImplTest extends AbstractJokerTest
{


    private final JokerConfig config = new JokerConfig();

    private final IdGenerator idGenerator = new IdGenerator();

    private final RegionDefFormerImpl regionDefFormer = new RegionDefFormerImpl( idGenerator );

    private final FlowDeploymentDefFormerImpl flowDeploymentDefFormer = new FlowDeploymentDefFormerImpl( config, idGenerator );

    private final PartitionService partitionService = new PartitionServiceImpl( config );

    private final OperatorKVStoreManagerImpl operatorKVStoreManager = new OperatorKVStoreManagerImpl( partitionService );

    private final OperatorTupleQueueManagerImpl operatorTupleQueueManager = new OperatorTupleQueueManagerImpl( config,
                                                                                                               partitionService,
                                                                                                               new PartitionKeyExtractorFactoryImpl() );

    private final PipelineTransformer pipelineTransformer = new PipelineTransformerImpl( config, operatorTupleQueueManager );

    private final RegionManagerImpl regionManager = new RegionManagerImpl( config, operatorKVStoreManager, operatorTupleQueueManager,
                                                                           pipelineTransformer );


    @Test
    public void test_statelessRegion_singlePipeline_singleReplica_noInputConnection ()
    {
        final Class<StatelessOperatorWithSingleInputOutputPortCount> operatorClazz = StatelessOperatorWithSingleInputOutputPortCount.class;
        final OperatorDef operatorDef0 = OperatorDefBuilder.newInstance( "op0", operatorClazz ).build();
        final OperatorDef operatorDef1 = OperatorDefBuilder.newInstance( "op1", operatorClazz ).build();

        final FlowDef flow = new FlowDefBuilder().add( operatorDef0 ).add( operatorDef1 ).connect( "op0", "op1" ).build();

        final List<RegionDef> regionDefs = regionDefFormer.createRegions( flow );
        final RegionDef regionDef = regionDefs.get( 0 );
        final RegionConfig regionConfig = new RegionConfig( regionDef, singletonList( 0 ), 1 );

        final Region region = regionManager.createRegion( flow, regionConfig );
        assertNotNull( region );
        final PipelineReplica[] pipelines = region.getReplicaPipelines( 0 );
        assertEquals( 1, pipelines.length );
        final PipelineReplica pipeline = pipelines[ 0 ];
        assertEmptyOperatorTupleQueue( pipeline, operatorDef0.inputPortCount() );

        assertStatelessPipelineWithNoInput( regionDef.getRegionId(), 0, 0, pipeline, operatorDef0, operatorDef1 );
    }

    @Test
    public void test_statelessRegion_multiPipeline_singleReplica_noInputConnection ()
    {
        final Class<StatelessOperatorWithSingleInputOutputPortCount> operatorClazz = StatelessOperatorWithSingleInputOutputPortCount.class;
        final OperatorDef operatorDef0 = OperatorDefBuilder.newInstance( "op0", operatorClazz ).build();
        final OperatorDef operatorDef1 = OperatorDefBuilder.newInstance( "op1", operatorClazz ).build();

        final FlowDef flow = new FlowDefBuilder().add( operatorDef0 ).add( operatorDef1 ).connect( "op0", "op1" ).build();

        final List<RegionDef> regionDefs = regionDefFormer.createRegions( flow );
        final RegionDef regionDef = regionDefs.get( 0 );
        final RegionConfig regionConfig = new RegionConfig( regionDef, asList( 0, 1 ), 1 );

        final Region region = regionManager.createRegion( flow, regionConfig );
        assertNotNull( region );
        final PipelineReplica[] pipelines = region.getReplicaPipelines( 0 );
        assertEquals( 2, pipelines.length );
        final PipelineReplica pipeline0 = pipelines[ 0 ];
        final PipelineReplica pipeline1 = pipelines[ 1 ];
        assertEmptyOperatorTupleQueue( pipeline0, operatorDef0.inputPortCount() );
        assertEmptySelfPipelinTupleQueue( pipeline0 );
        assertDefaultOperatorTupleQueue( pipeline1, operatorDef1.inputPortCount() );
        assertEmptySelfPipelinTupleQueue( pipeline1 );

        assertEquals( new PipelineReplicaId( new PipelineId( regionDef.getRegionId(), 0 ), 0 ), pipeline0.id() );
        assertEquals( new PipelineReplicaId( new PipelineId( regionDef.getRegionId(), 1 ), 0 ), pipeline1.id() );
        assertEquals( 1, pipeline0.getOperatorCount() );
        assertEquals( 1, pipeline1.getOperatorCount() );
        final OperatorReplica operatorReplica0 = pipeline0.getOperator( 0 );
        final OperatorReplica operatorReplica1 = pipeline1.getOperator( 0 );
        assertOperatorDef( operatorReplica0, operatorDef0 );
        assertOperatorDef( operatorReplica1, operatorDef1 );
        assertEmptyOperatorTupleQueue( operatorReplica0 );
        assertDefaultOperatorTupleQueue( operatorReplica1, operatorDef1.inputPortCount(), MULTI_THREADED );
        assertEmptytOperatorKVStore( operatorReplica0 );
        assertEmptytOperatorKVStore( operatorReplica1 );
        assertBlockingTupleQueueDrainerPool( operatorReplica0 );
        assertBlockingTupleQueueDrainerPool( operatorReplica1 );
        assertNonCachedTuplesImplSupplier( operatorReplica0 );
        assertNonCachedTuplesImplSupplier( operatorReplica1 );
    }

    @Test
    public void test_release_statelessRegion_multiPipeline_singleReplica_noInputConnection ()
    {
        final Class<StatelessOperatorWithSingleInputOutputPortCount> operatorClazz = StatelessOperatorWithSingleInputOutputPortCount.class;
        final OperatorDef operatorDef0 = OperatorDefBuilder.newInstance( "op0", operatorClazz ).build();
        final OperatorDef operatorDef1 = OperatorDefBuilder.newInstance( "op1", operatorClazz ).build();

        final FlowDef flow = new FlowDefBuilder().add( operatorDef0 ).add( operatorDef1 ).connect( "op0", "op1" ).build();

        final List<RegionDef> regionDefs = regionDefFormer.createRegions( flow );
        final RegionDef regionDef = regionDefs.get( 0 );
        final RegionConfig regionConfig = new RegionConfig( regionDef, asList( 0, 1 ), 1 );

        final Region region = regionManager.createRegion( flow, regionConfig );

        assertNotNull( operatorTupleQueueManager.getDefaultOperatorTupleQueue( region.getRegionId(), 0, operatorDef1 ) );

        regionManager.releaseRegion( region.getRegionId() );

        assertNull( operatorTupleQueueManager.getDefaultOperatorTupleQueue( region.getRegionId(), 0, operatorDef1 ) );
    }

    private void assertStatelessPipelineWithNoInput ( final int regionId,
                                                      final int replicaIndex,
                                                      final int pipelineId,
                                                      final PipelineReplica pipelineReplica,
                                                      final OperatorDef operatorDef0,
                                                      final OperatorDef operatorDef1 )
    {
        assertEquals( new PipelineReplicaId( new PipelineId( regionId, pipelineId ), replicaIndex ), pipelineReplica.id() );
        assertEquals( 2, pipelineReplica.getOperatorCount() );
        final OperatorReplica operatorReplica0 = pipelineReplica.getOperator( 0 );
        final OperatorReplica operatorReplica1 = pipelineReplica.getOperator( 1 );
        assertOperatorDef( operatorReplica0, operatorDef0 );
        assertOperatorDef( operatorReplica1, operatorDef1 );
        assertEmptyOperatorTupleQueue( operatorReplica0 );
        assertDefaultOperatorTupleQueue( operatorReplica1, operatorDef1.inputPortCount(), SINGLE_THREADED );
        assertEmptytOperatorKVStore( operatorReplica0 );
        assertEmptytOperatorKVStore( operatorReplica1 );
        assertBlockingTupleQueueDrainerPool( operatorReplica0 );
        assertNonBlockingTupleQueueDrainerPool( operatorReplica1 );
        assertCachedTuplesImplSupplier( operatorReplica0 );
        assertNonCachedTuplesImplSupplier( operatorReplica1 );
    }

    @Test
    public void test_statelessRegion_singlePipeline_singleReplica_withInputConnection ()
    {
        final Class<StatelessOperatorWithSingleInputOutputPortCount> operatorClazz = StatelessOperatorWithSingleInputOutputPortCount.class;
        final OperatorDef operatorDef0 = OperatorDefBuilder.newInstance( "op0", StatefulOperatorWithSingleInputOutputPortCount.class )
                                                           .build();
        final OperatorDef operatorDef1 = OperatorDefBuilder.newInstance( "op1", operatorClazz ).build();
        final OperatorDef operatorDef2 = OperatorDefBuilder.newInstance( "op2", operatorClazz ).build();

        final FlowDef flow = new FlowDefBuilder().add( operatorDef0 )
                                                 .add( operatorDef1 )
                                                 .add( operatorDef2 )
                                                 .connect( "op0", "op1" )
                                                 .connect( "op1", "op2" )
                                                 .build();

        final List<RegionDef> regionDefs = regionDefFormer.createRegions( flow );
        final RegionDef regionDef = regionDefs.get( 1 );
        final RegionConfig regionConfig = new RegionConfig( regionDef, singletonList( 0 ), 1 );

        final Region region = regionManager.createRegion( flow, regionConfig );
        assertNotNull( region );
        final PipelineReplica[] pipelines = region.getReplicaPipelines( 0 );
        assertEquals( 1, pipelines.length );
        final PipelineReplica pipeline = pipelines[ 0 ];
        assertDefaultOperatorTupleQueue( pipeline, operatorDef1.inputPortCount() );
        assertEmptySelfPipelinTupleQueue( pipeline );

        assertEquals( new PipelineReplicaId( new PipelineId( 1, 0 ), 0 ), pipeline.id() );
        assertEquals( 2, pipeline.getOperatorCount() );
        final OperatorReplica operatorReplica1 = pipeline.getOperator( 0 );
        final OperatorReplica operatorReplica2 = pipeline.getOperator( 1 );
        assertOperatorDef( operatorReplica1, operatorDef1 );
        assertOperatorDef( operatorReplica2, operatorDef2 );
        assertDefaultOperatorTupleQueue( operatorReplica1, operatorDef1.inputPortCount(), MULTI_THREADED );
        assertDefaultOperatorTupleQueue( operatorReplica2, operatorDef2.inputPortCount(), SINGLE_THREADED );
        assertEmptytOperatorKVStore( operatorReplica1 );
        assertEmptytOperatorKVStore( operatorReplica2 );
        assertBlockingTupleQueueDrainerPool( operatorReplica1 );
        assertNonBlockingTupleQueueDrainerPool( operatorReplica2 );
        assertCachedTuplesImplSupplier( operatorReplica1 );
        assertNonCachedTuplesImplSupplier( operatorReplica2 );
    }

    @Test
    public void test_statelessRegion_singlePipeline_multiReplica_withInputConnection ()
    {
        final Class<StatelessOperatorWithSingleInputOutputPortCount> operatorClazz = StatelessOperatorWithSingleInputOutputPortCount.class;
        final OperatorDef operatorDef0 = OperatorDefBuilder.newInstance( "op0", StatefulOperatorWithSingleInputOutputPortCount.class )
                                                           .build();
        final OperatorDef operatorDef1 = OperatorDefBuilder.newInstance( "op1", operatorClazz ).build();
        final OperatorDef operatorDef2 = OperatorDefBuilder.newInstance( "op2", operatorClazz ).build();

        final FlowDef flow = new FlowDefBuilder().add( operatorDef0 )
                                                 .add( operatorDef1 )
                                                 .add( operatorDef2 )
                                                 .connect( "op0", "op1" )
                                                 .connect( "op1", "op2" )
                                                 .build();

        final List<RegionDef> regionDefs = regionDefFormer.createRegions( flow );
        final RegionDef regionDef = regionDefs.get( 1 );
        final RegionConfig regionConfig = new RegionConfig( regionDef, singletonList( 0 ), 2 );

        final Region region = regionManager.createRegion( flow, regionConfig );
        assertNotNull( region );
        final PipelineReplica[] pipelineReplicas0 = region.getReplicaPipelines( 0 );
        final PipelineReplica[] pipelineReplicas1 = region.getReplicaPipelines( 1 );
        assertEquals( 1, pipelineReplicas0.length );
        assertEquals( 1, pipelineReplicas1.length );
        final PipelineReplica pipelineReplica0 = pipelineReplicas0[ 0 ];
        final PipelineReplica pipelineReplica1 = pipelineReplicas1[ 0 ];
        assertDefaultOperatorTupleQueue( pipelineReplica0, operatorDef1.inputPortCount() );
        assertEmptySelfPipelinTupleQueue( pipelineReplica0 );
        assertDefaultOperatorTupleQueue( pipelineReplica1, operatorDef1.inputPortCount() );
        assertEmptySelfPipelinTupleQueue( pipelineReplica1 );

        assertEquals( new PipelineReplicaId( new PipelineId( 1, 0 ), 0 ), pipelineReplica0.id() );
        assertEquals( 2, pipelineReplica0.getOperatorCount() );
        assertEquals( 2, pipelineReplica1.getOperatorCount() );
        final OperatorReplica operatorReplica0_1 = pipelineReplica0.getOperator( 0 );
        final OperatorReplica operatorReplica0_2 = pipelineReplica0.getOperator( 1 );
        final OperatorReplica operatorReplica1_1 = pipelineReplica1.getOperator( 0 );
        final OperatorReplica operatorReplica1_2 = pipelineReplica1.getOperator( 1 );
        assertOperatorDef( operatorReplica0_1, operatorDef1 );
        assertOperatorDef( operatorReplica0_2, operatorDef2 );
        assertOperatorDef( operatorReplica1_1, operatorDef1 );
        assertOperatorDef( operatorReplica1_2, operatorDef2 );
        assertDefaultOperatorTupleQueue( operatorReplica0_1, operatorDef1.inputPortCount(), MULTI_THREADED );
        assertDefaultOperatorTupleQueue( operatorReplica0_2, operatorDef2.inputPortCount(), SINGLE_THREADED );
        assertDefaultOperatorTupleQueue( operatorReplica1_1, operatorDef1.inputPortCount(), MULTI_THREADED );
        assertDefaultOperatorTupleQueue( operatorReplica1_2, operatorDef2.inputPortCount(), SINGLE_THREADED );
        assertEmptytOperatorKVStore( operatorReplica0_1 );
        assertEmptytOperatorKVStore( operatorReplica0_2 );
        assertEmptytOperatorKVStore( operatorReplica1_1 );
        assertEmptytOperatorKVStore( operatorReplica1_2 );
        assertBlockingTupleQueueDrainerPool( operatorReplica0_1 );
        assertNonBlockingTupleQueueDrainerPool( operatorReplica0_2 );
        assertBlockingTupleQueueDrainerPool( operatorReplica1_1 );
        assertNonBlockingTupleQueueDrainerPool( operatorReplica1_2 );
        assertCachedTuplesImplSupplier( operatorReplica0_1 );
        assertNonCachedTuplesImplSupplier( operatorReplica0_2 );
        assertCachedTuplesImplSupplier( operatorReplica1_1 );
        assertNonCachedTuplesImplSupplier( operatorReplica1_2 );
    }

    @Test
    public void test_statefulRegion_singlePipeline_singleReplica ()
    {
        final OperatorDef operatorDef0 = OperatorDefBuilder.newInstance( "op0", StatelessOperatorWithSingleInputOutputPortCount.class )
                                                           .build();
        final OperatorDef operatorDef1 = OperatorDefBuilder.newInstance( "op1", StatefulOperatorWithSingleInputOutputPortCount.class )
                                                           .build();

        final FlowDef flow = new FlowDefBuilder().add( operatorDef0 ).add( operatorDef1 ).connect( "op0", "op1" ).build();

        final List<RegionDef> regionDefs = regionDefFormer.createRegions( flow );
        final RegionDef regionDef = regionDefs.get( 1 );
        final RegionConfig regionConfig = new RegionConfig( regionDef, singletonList( 0 ), 1 );

        final Region region = regionManager.createRegion( flow, regionConfig );
        assertNotNull( region );
        final PipelineReplica[] pipelines = region.getReplicaPipelines( 0 );
        assertEquals( 1, pipelines.length );
        final PipelineReplica pipeline = pipelines[ 0 ];
        assertDefaultOperatorTupleQueue( pipeline, operatorDef1.inputPortCount() );
        assertEmptySelfPipelinTupleQueue( pipeline );

        assertEquals( new PipelineReplicaId( new PipelineId( 1, 0 ), 0 ), pipeline.id() );
        assertEquals( 1, pipeline.getOperatorCount() );

        final OperatorReplica operatorReplica1 = pipeline.getOperator( 0 );
        assertOperatorDef( operatorReplica1, operatorDef1 );
        assertDefaultOperatorTupleQueue( operatorReplica1, operatorDef1.inputPortCount(), MULTI_THREADED );
        assertDefaultOperatorKVStore( operatorReplica1 );
        assertBlockingTupleQueueDrainerPool( operatorReplica1 );
        assertNonCachedTuplesImplSupplier( operatorReplica1 );
    }

    @Test
    public void test_statefulRegion_singlePipeline_singleReplica_withStatelessOperator ()
    {
        final OperatorDef operatorDef0 = OperatorDefBuilder.newInstance( "op0", StatelessOperatorWithSingleInputOutputPortCount.class )
                                                           .build();
        final OperatorDef operatorDef1 = OperatorDefBuilder.newInstance( "op1", StatefulOperatorWithSingleInputOutputPortCount.class )
                                                           .build();

        final FlowDef initialFlow = new FlowDefBuilder().add( operatorDef0 ).add( operatorDef1 ).connect( "op0", "op1" ).build();

        final FlowDeploymentDef flowDeployment = flowDeploymentDefFormer.createFlowDeploymentDef( initialFlow,
                                                                                                  regionDefFormer.createRegions(
                                                                                                          initialFlow ) );

        final FlowDef flow = flowDeployment.getFlow();
        final List<RegionDef> regionDefs = flowDeployment.getRegions();
        final RegionDef regionDef = regionDefs.get( 0 );
        final RegionConfig regionConfig = new RegionConfig( regionDef, singletonList( 0 ), 1 );

        final Region region = regionManager.createRegion( flow, regionConfig );
        assertNotNull( region );
        final PipelineReplica[] pipelines = region.getReplicaPipelines( 0 );
        assertEquals( 1, pipelines.length );
        final PipelineReplica pipeline = pipelines[ 0 ];
        assertEmptyOperatorTupleQueue( pipeline, operatorDef1.inputPortCount() );
        assertEmptySelfPipelinTupleQueue( pipeline );

        assertEquals( 2, pipeline.getOperatorCount() );

        final OperatorReplica operator0 = pipeline.getOperator( 0 );
        assertOperatorDef( operator0, operatorDef0 );
        assertEmptyOperatorTupleQueue( operator0 );
        assertEmptytOperatorKVStore( operator0 );
        assertBlockingTupleQueueDrainerPool( operator0 );
        assertCachedTuplesImplSupplier( operator0 );

        final OperatorReplica operator1 = pipeline.getOperator( 1 );
        assertOperatorDef( operator1, operatorDef1 );
        assertDefaultOperatorTupleQueue( operator1, operatorDef1.inputPortCount(), SINGLE_THREADED );
        assertDefaultOperatorKVStore( operator1 );
        assertNonBlockingTupleQueueDrainerPool( operator1 );
        assertNonCachedTuplesImplSupplier( operator1 );
    }

    @Test
    public void test_release_statefulRegion_singlePipeline_singleReplica ()
    {
        final OperatorDef operatorDef0 = OperatorDefBuilder.newInstance( "op0", StatelessOperatorWithSingleInputOutputPortCount.class )
                                                           .build();
        final OperatorDef operatorDef1 = OperatorDefBuilder.newInstance( "op1", StatefulOperatorWithSingleInputOutputPortCount.class )
                                                           .build();

        final FlowDef initialFlow = new FlowDefBuilder().add( operatorDef0 ).add( operatorDef1 ).connect( "op0", "op1" ).build();

        final FlowDeploymentDef flowDeployment = flowDeploymentDefFormer.createFlowDeploymentDef( initialFlow,
                                                                                                  regionDefFormer.createRegions(
                                                                                                          initialFlow ) );

        final FlowDef flow = flowDeployment.getFlow();
        final List<RegionDef> regionDefs = flowDeployment.getRegions();
        final RegionDef regionDef = regionDefs.get( 0 );
        final RegionConfig regionConfig = new RegionConfig( regionDef, singletonList( 0 ), 1 );

        final Region region = regionManager.createRegion( flow, regionConfig );

        assertNotNull( operatorTupleQueueManager.getDefaultOperatorTupleQueue( region.getRegionId(), 0, operatorDef1 ) );
        assertNotNull( operatorKVStoreManager.getDefaultOperatorKVStore( region.getRegionId(), operatorDef1.id() ) );

        regionManager.releaseRegion( region.getRegionId() );

        assertNull( operatorTupleQueueManager.getDefaultOperatorTupleQueue( region.getRegionId(), 0, operatorDef1 ) );
        assertNull( operatorKVStoreManager.getDefaultOperatorKVStore( region.getRegionId(), operatorDef1.id() ) );
    }

    @Test
    public void test_release_statefulRegion_singlePipeline_singleReplica_withStatelessOperator ()
    {
        final OperatorDef operatorDef0 = OperatorDefBuilder.newInstance( "op0", StatelessOperatorWithSingleInputOutputPortCount.class )
                                                           .build();
        final OperatorDef operatorDef1 = OperatorDefBuilder.newInstance( "op1", StatefulOperatorWithSingleInputOutputPortCount.class )
                                                           .build();

        final FlowDef flow = new FlowDefBuilder().add( operatorDef0 ).add( operatorDef1 ).connect( "op0", "op1" ).build();

        final List<RegionDef> regionDefs = regionDefFormer.createRegions( flow );
        final RegionDef regionDef = regionDefs.get( 1 );
        final RegionConfig regionConfig = new RegionConfig( regionDef, singletonList( 0 ), 1 );

        final Region region = regionManager.createRegion( flow, regionConfig );

        assertNotNull( operatorTupleQueueManager.getDefaultOperatorTupleQueue( region.getRegionId(), 0, operatorDef1 ) );
        assertNotNull( operatorKVStoreManager.getDefaultOperatorKVStore( region.getRegionId(), operatorDef1.id() ) );

        regionManager.releaseRegion( region.getRegionId() );

        assertNull( operatorTupleQueueManager.getDefaultOperatorTupleQueue( region.getRegionId(), 0, operatorDef1 ) );
        assertNull( operatorKVStoreManager.getDefaultOperatorKVStore( region.getRegionId(), operatorDef1.id() ) );
    }


    @Test
    public void test_partitionedStatefulRegion_singlePipeline_singleReplica ()
    {
        final OperatorRuntimeSchemaBuilder operatorRuntimeSchemaBuilder0 = new OperatorRuntimeSchemaBuilder( 1, 1 );
        operatorRuntimeSchemaBuilder0.addInputField( 0, "field1", Integer.class ).addOutputField( 0, "field2", Integer.class );
        final OperatorRuntimeSchemaBuilder operatorRuntimeSchemaBuilder1 = new OperatorRuntimeSchemaBuilder( 2, 1 );
        operatorRuntimeSchemaBuilder1.addInputField( 0, "field2", Integer.class )
                                     .addInputField( 1, "field2", Integer.class )
                                     .addOutputField( 0, "field3", Integer.class );
        final OperatorDef operatorDef0 = OperatorDefBuilder.newInstance( "op0", StatefulOperatorWithSingleInputOutputPortCount.class )
                                                           .setExtendingSchema( operatorRuntimeSchemaBuilder0 )
                                                           .build();
        final OperatorDef operatorDef1 = OperatorDefBuilder.newInstance( "op1",
                                                                         PartitionedStatefulOperatorWithSingleInputOutputPortCount.class )
                                                           .setExtendingSchema( operatorRuntimeSchemaBuilder1 )
                                                           .setPartitionFieldNames( singletonList( "field2" ) )
                                                           .build();

        final FlowDef flow = new FlowDefBuilder().add( operatorDef0 ).add( operatorDef1 ).connect( "op0", "op1" ).build();

        final List<RegionDef> regionDefs = regionDefFormer.createRegions( flow );
        final RegionDef regionDef = regionDefs.get( 1 );
        final RegionConfig regionConfig = new RegionConfig( regionDef, singletonList( 0 ), 1 );

        final Region region = regionManager.createRegion( flow, regionConfig );
        assertNotNull( region );
        final PipelineReplica[] pipelines = region.getReplicaPipelines( 0 );
        assertEquals( 1, pipelines.length );
        final PipelineReplica pipeline = pipelines[ 0 ];
        assertDefaultOperatorTupleQueue( pipeline, operatorDef1.inputPortCount() );
        assertDefaultSelfPipelineTupleQueue( pipeline );

        assertEquals( new PipelineReplicaId( new PipelineId( 1, 0 ), 0 ), pipeline.id() );
        assertEquals( 1, pipeline.getOperatorCount() );

        final OperatorReplica operatorReplica1 = pipeline.getOperator( 0 );
        assertOperatorDef( operatorReplica1, operatorDef1 );
        assertPartitionedOperatorTupleQueue( operatorReplica1 );
        assertPartitionedOperatorKVStore( operatorReplica1 );
        assertBlockingTupleQueueDrainerPool( operatorReplica1 );
        assertNonCachedTuplesImplSupplier( operatorReplica1 );
    }

    @Test
    public void test_release_partitionedStatefulRegion_singlePipeline_singleReplica ()
    {
        final OperatorRuntimeSchemaBuilder operatorRuntimeSchemaBuilder0 = new OperatorRuntimeSchemaBuilder( 1, 1 );
        operatorRuntimeSchemaBuilder0.addInputField( 0, "field1", Integer.class ).addOutputField( 0, "field2", Integer.class );
        final OperatorRuntimeSchemaBuilder operatorRuntimeSchemaBuilder1 = new OperatorRuntimeSchemaBuilder( 2, 1 );
        operatorRuntimeSchemaBuilder1.addInputField( 0, "field2", Integer.class )
                                     .addInputField( 1, "field2", Integer.class )
                                     .addOutputField( 0, "field3", Integer.class );
        final OperatorDef operatorDef0 = OperatorDefBuilder.newInstance( "op0", StatefulOperatorWithSingleInputOutputPortCount.class )
                                                           .setExtendingSchema( operatorRuntimeSchemaBuilder0 )
                                                           .build();
        final OperatorDef operatorDef1 = OperatorDefBuilder.newInstance( "op1",
                                                                         PartitionedStatefulOperatorWithSingleInputOutputPortCount.class )
                                                           .setExtendingSchema( operatorRuntimeSchemaBuilder1 )
                                                           .setPartitionFieldNames( singletonList( "field2" ) )
                                                           .build();

        final FlowDef flow = new FlowDefBuilder().add( operatorDef0 ).add( operatorDef1 ).connect( "op0", "op1" ).build();

        final List<RegionDef> regionDefs = regionDefFormer.createRegions( flow );
        final RegionDef regionDef = regionDefs.get( 1 );
        final RegionConfig regionConfig = new RegionConfig( regionDef, singletonList( 0 ), 1 );

        final Region region = regionManager.createRegion( flow, regionConfig );

        final OperatorKVStore[] operatorKvStores = operatorKVStoreManager.getPartitionedOperatorKVStore( region.getRegionId(),
                                                                                                         operatorDef1.id() );
        assertNotNull( operatorKvStores );
        assertEquals( 1, operatorKvStores.length );

        final PartitionedOperatorTupleQueue[] operatorTupleQueues = operatorTupleQueueManager.getPartitionedOperatorTupleQueues( region.getRegionId(),
                                                                                                                                 operatorDef1 );

        assertNotNull( operatorTupleQueues );
        assertEquals( 1, operatorTupleQueues.length );

        regionManager.releaseRegion( region.getRegionId() );

        assertNull( operatorKVStoreManager.getPartitionedOperatorKVStore( region.getRegionId(), operatorDef1.id() ) );
        assertNull( operatorTupleQueueManager.getPartitionedOperatorTupleQueues( region.getRegionId(), operatorDef1 ) );
    }

    @Test
    public void test_partitionedStatefulRegion_singlePipeline_multiReplica ()
    {
        final OperatorRuntimeSchemaBuilder operatorRuntimeSchemaBuilder0 = new OperatorRuntimeSchemaBuilder( 1, 1 );
        operatorRuntimeSchemaBuilder0.addInputField( 0, "field1", Integer.class ).addOutputField( 0, "field2", Integer.class );
        final OperatorRuntimeSchemaBuilder operatorRuntimeSchemaBuilder1 = new OperatorRuntimeSchemaBuilder( 2, 1 );
        operatorRuntimeSchemaBuilder1.addInputField( 0, "field2", Integer.class )
                                     .addInputField( 1, "field2", Integer.class )
                                     .addOutputField( 0, "field3", Integer.class );
        final OperatorDef operatorDef0 = OperatorDefBuilder.newInstance( "op0", StatefulOperatorWithSingleInputOutputPortCount.class )
                                                           .setExtendingSchema( operatorRuntimeSchemaBuilder0 )
                                                           .build();
        final OperatorDef operatorDef1 = OperatorDefBuilder.newInstance( "op1",
                                                                         PartitionedStatefulOperatorWithSingleInputOutputPortCount.class )
                                                           .setExtendingSchema( operatorRuntimeSchemaBuilder1 )
                                                           .setPartitionFieldNames( singletonList( "field2" ) )
                                                           .build();

        final FlowDef flow = new FlowDefBuilder().add( operatorDef0 ).add( operatorDef1 ).connect( "op0", "op1" ).build();

        final List<RegionDef> regionDefs = regionDefFormer.createRegions( flow );
        final RegionDef regionDef = regionDefs.get( 1 );
        final RegionConfig regionConfig = new RegionConfig( regionDef, singletonList( 0 ), 2 );

        final Region region = regionManager.createRegion( flow, regionConfig );
        assertNotNull( region );
        final PipelineReplica[] pipelines0 = region.getReplicaPipelines( 0 );
        final PipelineReplica[] pipelines1 = region.getReplicaPipelines( 1 );
        assertEquals( 1, pipelines0.length );
        assertEquals( 1, pipelines1.length );
        final PipelineReplica pipelineReplica0 = pipelines0[ 0 ];
        final PipelineReplica pipelineReplica1 = pipelines1[ 0 ];
        assertDefaultOperatorTupleQueue( pipelineReplica0, operatorDef1.inputPortCount() );
        assertDefaultSelfPipelineTupleQueue( pipelineReplica0 );
        assertDefaultOperatorTupleQueue( pipelineReplica1, operatorDef1.inputPortCount() );
        assertDefaultSelfPipelineTupleQueue( pipelineReplica1 );

        assertEquals( new PipelineReplicaId( new PipelineId( 1, 0 ), 0 ), pipelineReplica0.id() );
        assertEquals( new PipelineReplicaId( new PipelineId( 1, 0 ), 1 ), pipelineReplica1.id() );
        assertEquals( 1, pipelineReplica0.getOperatorCount() );
        assertEquals( 1, pipelineReplica1.getOperatorCount() );

        final OperatorReplica operatorReplica0 = pipelineReplica0.getOperator( 0 );
        final OperatorReplica operatorReplica1 = pipelineReplica1.getOperator( 0 );
        assertOperatorDef( operatorReplica0, operatorDef1 );
        assertOperatorDef( operatorReplica1, operatorDef1 );
        assertPartitionedOperatorTupleQueue( operatorReplica0 );
        assertPartitionedOperatorTupleQueue( operatorReplica1 );
        assertPartitionedOperatorKVStore( operatorReplica0 );
        assertPartitionedOperatorKVStore( operatorReplica1 );
        assertBlockingTupleQueueDrainerPool( operatorReplica0 );
        assertBlockingTupleQueueDrainerPool( operatorReplica1 );
        assertNonCachedTuplesImplSupplier( operatorReplica0 );
        assertNonCachedTuplesImplSupplier( operatorReplica1 );
    }

    @Test
    public void test_partitionedStatefulRegion_singlePipeline_singleReplica_withStatelessOperatorFirst ()
    {
        final OperatorRuntimeSchemaBuilder operatorRuntimeSchemaBuilder0 = new OperatorRuntimeSchemaBuilder( 1, 1 );
        operatorRuntimeSchemaBuilder0.addInputField( 0, "field1", Integer.class ).addOutputField( 0, "field2", Integer.class );
        final OperatorRuntimeSchemaBuilder operatorRuntimeSchemaBuilder1 = new OperatorRuntimeSchemaBuilder( 1, 1 );
        operatorRuntimeSchemaBuilder1.addInputField( 0, "field2", Integer.class ).addOutputField( 0, "field2", Integer.class );
        final OperatorRuntimeSchemaBuilder operatorRuntimeSchemaBuilder2 = new OperatorRuntimeSchemaBuilder( 2, 1 );
        operatorRuntimeSchemaBuilder2.addInputField( 0, "field2", Integer.class )
                                     .addInputField( 1, "field2", Integer.class )
                                     .addOutputField( 0, "field3", Integer.class );
        final OperatorDef operatorDef0 = OperatorDefBuilder.newInstance( "op0", StatefulOperatorWithSingleInputOutputPortCount.class )
                                                           .setExtendingSchema( operatorRuntimeSchemaBuilder0 )
                                                           .build();
        final OperatorDef operatorDef1 = OperatorDefBuilder.newInstance( "op1", StatelessOperatorWithSingleInputOutputPortCount.class )
                                                           .setExtendingSchema( operatorRuntimeSchemaBuilder1 )
                                                           .build();
        final OperatorDef operatorDef2 = OperatorDefBuilder.newInstance( "op2",
                                                                         PartitionedStatefulOperatorWithSingleInputOutputPortCount.class )
                                                           .setExtendingSchema( operatorRuntimeSchemaBuilder2 )
                                                           .setPartitionFieldNames( singletonList( "field2" ) )
                                                           .build();

        final FlowDef flow = new FlowDefBuilder().add( operatorDef0 )
                                                 .add( operatorDef1 )
                                                 .add( operatorDef2 )
                                                 .connect( "op0", "op1" )
                                                 .connect( "op1", "op2" )
                                                 .build();

        final List<RegionDef> regionDefs = regionDefFormer.createRegions( flow );
        final RegionDef regionDef = regionDefs.get( 1 );
        final RegionConfig regionConfig = new RegionConfig( regionDef, singletonList( 0 ), 1 );

        final Region region = regionManager.createRegion( flow, regionConfig );
        assertNotNull( region );
        final PipelineReplica[] pipelines = region.getReplicaPipelines( 0 );
        assertEquals( 1, pipelines.length );
        final PipelineReplica pipeline = pipelines[ 0 ];
        assertDefaultOperatorTupleQueue( pipeline, operatorDef1.inputPortCount() );
        assertEmptySelfPipelinTupleQueue( pipeline );

        assertEquals( new PipelineReplicaId( new PipelineId( 1, 0 ), 0 ), pipeline.id() );
        assertEquals( 2, pipeline.getOperatorCount() );

        final OperatorReplica operatorReplica1 = pipeline.getOperator( 0 );
        assertOperatorDef( operatorReplica1, operatorDef1 );
        assertDefaultOperatorTupleQueue( operatorReplica1, operatorDef1.inputPortCount(), MULTI_THREADED );
        assertEmptytOperatorKVStore( operatorReplica1 );
        assertBlockingTupleQueueDrainerPool( operatorReplica1 );
        assertCachedTuplesImplSupplier( operatorReplica1 );

        final OperatorReplica operatorReplica2 = pipeline.getOperator( 1 );
        assertOperatorDef( operatorReplica2, operatorDef2 );
        assertPartitionedOperatorTupleQueue( operatorReplica2 );
        assertPartitionedOperatorKVStore( operatorReplica2 );
        assertNonBlockingTupleQueueDrainerPool( operatorReplica2 );
        assertNonCachedTuplesImplSupplier( operatorReplica2 );
    }

    @Test
    public void test_partitionedStatefulRegion_singlePipeline_multiReplica_withStatelessOperatorFirst ()
    {
        final OperatorRuntimeSchemaBuilder operatorRuntimeSchemaBuilder0 = new OperatorRuntimeSchemaBuilder( 1, 1 );
        operatorRuntimeSchemaBuilder0.addInputField( 0, "field1", Integer.class ).addOutputField( 0, "field2", Integer.class );
        final OperatorRuntimeSchemaBuilder operatorRuntimeSchemaBuilder1 = new OperatorRuntimeSchemaBuilder( 1, 1 );
        operatorRuntimeSchemaBuilder1.addInputField( 0, "field2", Integer.class ).addOutputField( 0, "field2", Integer.class );
        final OperatorRuntimeSchemaBuilder operatorRuntimeSchemaBuilder2 = new OperatorRuntimeSchemaBuilder( 2, 1 );
        operatorRuntimeSchemaBuilder2.addInputField( 0, "field2", Integer.class )
                                     .addInputField( 1, "field2", Integer.class )
                                     .addOutputField( 0, "field3", Integer.class );
        final OperatorDef operatorDef0 = OperatorDefBuilder.newInstance( "op0", StatefulOperatorWithSingleInputOutputPortCount.class )
                                                           .setExtendingSchema( operatorRuntimeSchemaBuilder0 )
                                                           .build();
        final OperatorDef operatorDef1 = OperatorDefBuilder.newInstance( "op1", StatelessOperatorWithSingleInputOutputPortCount.class )
                                                           .setExtendingSchema( operatorRuntimeSchemaBuilder1 )
                                                           .build();
        final OperatorDef operatorDef2 = OperatorDefBuilder.newInstance( "op2",
                                                                         PartitionedStatefulOperatorWithSingleInputOutputPortCount.class )
                                                           .setExtendingSchema( operatorRuntimeSchemaBuilder2 )
                                                           .setPartitionFieldNames( singletonList( "field2" ) )
                                                           .build();

        final FlowDef flow = new FlowDefBuilder().add( operatorDef0 )
                                                 .add( operatorDef1 )
                                                 .add( operatorDef2 )
                                                 .connect( "op0", "op1" )
                                                 .connect( "op1", "op2" )
                                                 .build();

        final List<RegionDef> regionDefs = regionDefFormer.createRegions( flow );
        final RegionDef regionDef = regionDefs.get( 1 );
        final RegionConfig regionConfig = new RegionConfig( regionDef, singletonList( 0 ), 2 );

        final Region region = regionManager.createRegion( flow, regionConfig );
        assertNotNull( region );
        final PipelineReplica[] pipelinesReplica0 = region.getReplicaPipelines( 0 );
        final PipelineReplica[] pipelinesReplica1 = region.getReplicaPipelines( 1 );
        assertEquals( 1, pipelinesReplica0.length );
        assertEquals( 1, pipelinesReplica1.length );
        final PipelineReplica pipelineReplica0 = pipelinesReplica0[ 0 ];
        final PipelineReplica pipelineReplica1 = pipelinesReplica1[ 0 ];
        assertDefaultOperatorTupleQueue( pipelineReplica0, operatorDef1.inputPortCount() );
        assertEmptySelfPipelinTupleQueue( pipelineReplica0 );
        assertDefaultOperatorTupleQueue( pipelineReplica1, operatorDef1.inputPortCount() );
        assertEmptySelfPipelinTupleQueue( pipelineReplica1 );
        assertFalse( pipelineReplica0.getPipelineTupleQueue() == pipelineReplica1.getPipelineTupleQueue() );

        assertEquals( new PipelineReplicaId( new PipelineId( 1, 0 ), 0 ), pipelineReplica0.id() );
        assertEquals( new PipelineReplicaId( new PipelineId( 1, 0 ), 1 ), pipelineReplica1.id() );
        assertEquals( 2, pipelineReplica0.getOperatorCount() );
        assertEquals( 2, pipelineReplica1.getOperatorCount() );

        final OperatorReplica operatorReplica0_1 = pipelineReplica0.getOperator( 0 );
        assertOperatorDef( operatorReplica0_1, operatorDef1 );
        assertDefaultOperatorTupleQueue( operatorReplica0_1, operatorDef1.inputPortCount(), MULTI_THREADED );
        assertEmptytOperatorKVStore( operatorReplica0_1 );
        assertBlockingTupleQueueDrainerPool( operatorReplica0_1 );
        assertCachedTuplesImplSupplier( operatorReplica0_1 );

        final OperatorReplica operatorReplica1_1 = pipelineReplica0.getOperator( 0 );
        assertOperatorDef( operatorReplica1_1, operatorDef1 );
        assertDefaultOperatorTupleQueue( operatorReplica1_1, operatorDef1.inputPortCount(), MULTI_THREADED );
        assertEmptytOperatorKVStore( operatorReplica1_1 );
        assertBlockingTupleQueueDrainerPool( operatorReplica1_1 );
        assertCachedTuplesImplSupplier( operatorReplica1_1 );

        final OperatorReplica operatorReplica0_2 = pipelineReplica0.getOperator( 1 );
        assertOperatorDef( operatorReplica0_2, operatorDef2 );
        assertPartitionedOperatorTupleQueue( operatorReplica0_2 );
        assertPartitionedOperatorKVStore( operatorReplica0_2 );
        assertNonBlockingTupleQueueDrainerPool( operatorReplica0_2 );
        assertNonCachedTuplesImplSupplier( operatorReplica0_2 );

        final OperatorReplica operatorReplica1_2 = pipelineReplica0.getOperator( 1 );
        assertOperatorDef( operatorReplica1_2, operatorDef2 );
        assertPartitionedOperatorTupleQueue( operatorReplica1_2 );
        assertPartitionedOperatorKVStore( operatorReplica1_2 );
        assertNonBlockingTupleQueueDrainerPool( operatorReplica1_2 );
        assertNonCachedTuplesImplSupplier( operatorReplica1_2 );
    }

    @Test
    public void test_release_partitionedStatefulRegion_singlePipeline_multiReplica_withStatelessOperatorFirst ()
    {
        final OperatorRuntimeSchemaBuilder operatorRuntimeSchemaBuilder0 = new OperatorRuntimeSchemaBuilder( 1, 1 );
        operatorRuntimeSchemaBuilder0.addInputField( 0, "field1", Integer.class ).addOutputField( 0, "field2", Integer.class );
        final OperatorRuntimeSchemaBuilder operatorRuntimeSchemaBuilder1 = new OperatorRuntimeSchemaBuilder( 1, 1 );
        operatorRuntimeSchemaBuilder1.addInputField( 0, "field2", Integer.class ).addOutputField( 0, "field2", Integer.class );
        final OperatorRuntimeSchemaBuilder operatorRuntimeSchemaBuilder2 = new OperatorRuntimeSchemaBuilder( 2, 1 );
        operatorRuntimeSchemaBuilder2.addInputField( 0, "field2", Integer.class )
                                     .addInputField( 1, "field2", Integer.class )
                                     .addOutputField( 0, "field3", Integer.class );
        final OperatorDef operatorDef0 = OperatorDefBuilder.newInstance( "op0", StatefulOperatorWithSingleInputOutputPortCount.class )
                                                           .setExtendingSchema( operatorRuntimeSchemaBuilder0 )
                                                           .build();
        final OperatorDef operatorDef1 = OperatorDefBuilder.newInstance( "op1", StatelessOperatorWithSingleInputOutputPortCount.class )
                                                           .setExtendingSchema( operatorRuntimeSchemaBuilder1 )
                                                           .build();
        final OperatorDef operatorDef2 = OperatorDefBuilder.newInstance( "op2",
                                                                         PartitionedStatefulOperatorWithSingleInputOutputPortCount.class )
                                                           .setExtendingSchema( operatorRuntimeSchemaBuilder2 )
                                                           .setPartitionFieldNames( singletonList( "field2" ) )
                                                           .build();

        final FlowDef flow = new FlowDefBuilder().add( operatorDef0 )
                                                 .add( operatorDef1 )
                                                 .add( operatorDef2 )
                                                 .connect( "op0", "op1" )
                                                 .connect( "op1", "op2" )
                                                 .build();

        final List<RegionDef> regionDefs = regionDefFormer.createRegions( flow );
        final RegionDef regionDef = regionDefs.get( 1 );
        final RegionConfig regionConfig = new RegionConfig( regionDef, singletonList( 0 ), 2 );

        final Region region = regionManager.createRegion( flow, regionConfig );

        assertNotNull( operatorTupleQueueManager.getDefaultOperatorTupleQueue( region.getRegionId(), 0, operatorDef1 ) );
        assertNotNull( operatorTupleQueueManager.getDefaultOperatorTupleQueue( region.getRegionId(), 1, operatorDef1 ) );
        final PartitionedOperatorTupleQueue[] operatorTupleQueues = operatorTupleQueueManager.getPartitionedOperatorTupleQueues( region.getRegionId(),
                                                                                                                                 operatorDef2 );
        assertNotNull( operatorTupleQueues );
        assertEquals( 2, operatorTupleQueues.length );

        final OperatorKVStore[] operatorKvStores = operatorKVStoreManager.getPartitionedOperatorKVStore( region.getRegionId(),
                                                                                                         operatorDef2.id() );
        assertNotNull( operatorKvStores );
        assertEquals( 2, operatorKvStores.length );

        regionManager.releaseRegion( region.getRegionId() );

        assertNull( operatorTupleQueueManager.getDefaultOperatorTupleQueue( region.getRegionId(), 0, operatorDef1 ) );
        assertNull( operatorTupleQueueManager.getDefaultOperatorTupleQueue( region.getRegionId(), 1, operatorDef1 ) );
        assertNull( operatorKVStoreManager.getPartitionedOperatorKVStore( region.getRegionId(), operatorDef2.id() ) );
    }

    @Test
    public void test_partitionedStatefulRegion_twoPipelines_singleReplica_withStatelessOperatorFirst ()
    {
        final OperatorRuntimeSchemaBuilder operatorRuntimeSchemaBuilder0 = new OperatorRuntimeSchemaBuilder( 1, 1 );
        operatorRuntimeSchemaBuilder0.addInputField( 0, "field1", Integer.class ).addOutputField( 0, "field2", Integer.class );
        final OperatorRuntimeSchemaBuilder operatorRuntimeSchemaBuilder1 = new OperatorRuntimeSchemaBuilder( 1, 1 );
        operatorRuntimeSchemaBuilder1.addInputField( 0, "field2", Integer.class ).addOutputField( 0, "field2", Integer.class );
        final OperatorRuntimeSchemaBuilder operatorRuntimeSchemaBuilder2 = new OperatorRuntimeSchemaBuilder( 2, 1 );
        operatorRuntimeSchemaBuilder2.addInputField( 0, "field2", Integer.class )
                                     .addInputField( 1, "field2", Integer.class )
                                     .addOutputField( 0, "field3", Integer.class );
        final OperatorRuntimeSchemaBuilder operatorRuntimeSchemaBuilder3 = new OperatorRuntimeSchemaBuilder( 1, 1 );
        operatorRuntimeSchemaBuilder3.addInputField( 0, "field3", Integer.class ).addOutputField( 0, "field3", Integer.class );
        final OperatorDef operatorDef0 = OperatorDefBuilder.newInstance( "op0", StatefulOperatorWithSingleInputOutputPortCount.class )
                                                           .setExtendingSchema( operatorRuntimeSchemaBuilder0 )
                                                           .build();
        final OperatorDef operatorDef1 = OperatorDefBuilder.newInstance( "op1", StatelessOperatorWithSingleInputOutputPortCount.class )
                                                           .setExtendingSchema( operatorRuntimeSchemaBuilder1 )
                                                           .build();
        final OperatorDef operatorDef2 = OperatorDefBuilder.newInstance( "op2",
                                                                         PartitionedStatefulOperatorWithSingleInputOutputPortCount.class )
                                                           .setExtendingSchema( operatorRuntimeSchemaBuilder2 )
                                                           .setPartitionFieldNames( singletonList( "field2" ) )
                                                           .build();
        final OperatorDef operatorDef3 = OperatorDefBuilder.newInstance( "op3", StatelessOperatorWithSingleInputOutputPortCount.class )
                                                           .setExtendingSchema( operatorRuntimeSchemaBuilder3 )
                                                           .build();

        final FlowDef flow = new FlowDefBuilder().add( operatorDef0 )
                                                 .add( operatorDef1 )
                                                 .add( operatorDef2 )
                                                 .add( operatorDef3 )
                                                 .connect( "op0", "op1" )
                                                 .connect( "op1", "op2" )
                                                 .connect( "op2", "op3" )
                                                 .build();

        final List<RegionDef> regionDefs = regionDefFormer.createRegions( flow );
        final RegionDef regionDef = regionDefs.get( 1 );
        final RegionConfig regionConfig = new RegionConfig( regionDef, asList( 0, 1 ), 1 );

        final Region region = regionManager.createRegion( flow, regionConfig );
        assertNotNull( region );
        final PipelineReplica[] pipelines = region.getReplicaPipelines( 0 );
        assertEquals( 2, pipelines.length );
        final PipelineReplica pipeline0 = pipelines[ 0 ];
        final PipelineReplica pipeline1 = pipelines[ 1 ];
        assertDefaultOperatorTupleQueue( pipeline0, operatorDef1.inputPortCount() );
        assertEmptySelfPipelinTupleQueue( pipeline0 );
        assertDefaultOperatorTupleQueue( pipeline1, operatorDef2.inputPortCount() );
        assertDefaultSelfPipelineTupleQueue( pipeline1 );

        assertEquals( new PipelineReplicaId( new PipelineId( 1, 0 ), 0 ), pipeline0.id() );
        assertEquals( new PipelineReplicaId( new PipelineId( 1, 1 ), 0 ), pipeline1.id() );
        assertEquals( 1, pipeline0.getOperatorCount() );
        assertEquals( 2, pipeline1.getOperatorCount() );

        final OperatorReplica operatorReplica1 = pipeline0.getOperator( 0 );
        assertOperatorDef( operatorReplica1, operatorDef1 );
        assertDefaultOperatorTupleQueue( operatorReplica1, operatorDef1.inputPortCount(), MULTI_THREADED );
        assertEmptytOperatorKVStore( operatorReplica1 );
        assertBlockingTupleQueueDrainerPool( operatorReplica1 );
        assertNonCachedTuplesImplSupplier( operatorReplica1 );

        final OperatorReplica operatorReplica2 = pipeline1.getOperator( 0 );
        assertOperatorDef( operatorReplica2, operatorDef2 );
        assertPartitionedOperatorTupleQueue( operatorReplica2 );
        assertPartitionedOperatorKVStore( operatorReplica2 );
        assertBlockingTupleQueueDrainerPool( operatorReplica2 );
        assertCachedTuplesImplSupplier( operatorReplica2 );

        final OperatorReplica operatorReplica3 = pipeline1.getOperator( 1 );
        assertOperatorDef( operatorReplica3, operatorDef3 );
        assertDefaultOperatorTupleQueue( operatorReplica3, operatorDef3.inputPortCount(), SINGLE_THREADED );
        assertEmptytOperatorKVStore( operatorReplica3 );
        assertNonBlockingTupleQueueDrainerPool( operatorReplica3 );
        assertNonCachedTuplesImplSupplier( operatorReplica3 );
    }

    @Test
    public void test_partitionedStatefulRegion_twoPipelines_singleReplica_bothPipelinesStartWithStatelessOperator ()
    {
        final OperatorRuntimeSchemaBuilder operatorRuntimeSchemaBuilder0 = new OperatorRuntimeSchemaBuilder( 1, 1 );
        operatorRuntimeSchemaBuilder0.addInputField( 0, "field1", Integer.class ).addOutputField( 0, "field2", Integer.class );
        final OperatorRuntimeSchemaBuilder operatorRuntimeSchemaBuilder1 = new OperatorRuntimeSchemaBuilder( 1, 1 );
        operatorRuntimeSchemaBuilder1.addInputField( 0, "field2", Integer.class ).addOutputField( 0, "field2", Integer.class );
        final OperatorRuntimeSchemaBuilder operatorRuntimeSchemaBuilder2 = new OperatorRuntimeSchemaBuilder( 2, 1 );
        operatorRuntimeSchemaBuilder2.addInputField( 0, "field2", Integer.class )
                                     .addInputField( 1, "field2", Integer.class )
                                     .addOutputField( 0, "field3", Integer.class );
        final OperatorRuntimeSchemaBuilder operatorRuntimeSchemaBuilder3 = new OperatorRuntimeSchemaBuilder( 1, 1 );
        operatorRuntimeSchemaBuilder3.addInputField( 0, "field3", Integer.class ).addOutputField( 0, "field3", Integer.class );
        final OperatorDef operatorDef0 = OperatorDefBuilder.newInstance( "op0", StatefulOperatorWithSingleInputOutputPortCount.class )
                                                           .setExtendingSchema( operatorRuntimeSchemaBuilder0 )
                                                           .build();
        final OperatorDef operatorDef1 = OperatorDefBuilder.newInstance( "op1", StatelessOperatorWithSingleInputOutputPortCount.class )
                                                           .setExtendingSchema( operatorRuntimeSchemaBuilder1 )
                                                           .build();
        final OperatorDef operatorDef2 = OperatorDefBuilder.newInstance( "op2",
                                                                         PartitionedStatefulOperatorWithSingleInputOutputPortCount.class )
                                                           .setExtendingSchema( operatorRuntimeSchemaBuilder2 )
                                                           .setPartitionFieldNames( singletonList( "field2" ) )
                                                           .build();
        final OperatorDef operatorDef3 = OperatorDefBuilder.newInstance( "op3", StatelessOperatorWithSingleInputOutputPortCount.class )
                                                           .setExtendingSchema( operatorRuntimeSchemaBuilder3 )
                                                           .build();

        final FlowDef flow = new FlowDefBuilder().add( operatorDef0 )
                                                 .add( operatorDef1 )
                                                 .add( operatorDef2 )
                                                 .add( operatorDef3 )
                                                 .connect( "op0", "op1" )
                                                 .connect( "op1", "op2" )
                                                 .connect( "op2", "op3" )
                                                 .build();

        final List<RegionDef> regionDefs = regionDefFormer.createRegions( flow );
        final RegionDef regionDef = regionDefs.get( 1 );
        final RegionConfig regionConfig = new RegionConfig( regionDef, asList( 0, 2 ), 1 );

        final Region region = regionManager.createRegion( flow, regionConfig );
        assertNotNull( region );
        final PipelineReplica[] pipelines = region.getReplicaPipelines( 0 );
        assertEquals( 2, pipelines.length );
        final PipelineReplica pipeline0 = pipelines[ 0 ];
        final PipelineReplica pipeline1 = pipelines[ 1 ];
        assertDefaultOperatorTupleQueue( pipeline0, operatorDef1.inputPortCount() );
        assertEmptySelfPipelinTupleQueue( pipeline0 );
        assertDefaultOperatorTupleQueue( pipeline1, operatorDef3.inputPortCount() );
        assertEmptySelfPipelinTupleQueue( pipeline0 );

        assertEquals( new PipelineReplicaId( new PipelineId( 1, 0 ), 0 ), pipeline0.id() );
        assertEquals( new PipelineReplicaId( new PipelineId( 1, 2 ), 0 ), pipeline1.id() );
        assertEquals( 2, pipeline0.getOperatorCount() );
        assertEquals( 1, pipeline1.getOperatorCount() );

        final OperatorReplica operatorReplica1 = pipeline0.getOperator( 0 );
        assertOperatorDef( operatorReplica1, operatorDef1 );
        assertDefaultOperatorTupleQueue( operatorReplica1, operatorDef1.inputPortCount(), MULTI_THREADED );
        assertEmptytOperatorKVStore( operatorReplica1 );
        assertBlockingTupleQueueDrainerPool( operatorReplica1 );
        assertCachedTuplesImplSupplier( operatorReplica1 );

        final OperatorReplica operatorReplica2 = pipeline0.getOperator( 1 );
        assertOperatorDef( operatorReplica2, operatorDef2 );
        assertPartitionedOperatorTupleQueue( operatorReplica2 );
        assertPartitionedOperatorKVStore( operatorReplica2 );
        assertNonBlockingTupleQueueDrainerPool( operatorReplica2 );
        assertNonCachedTuplesImplSupplier( operatorReplica2 );

        final OperatorReplica operatorReplica3 = pipeline1.getOperator( 0 );
        assertOperatorDef( operatorReplica3, operatorDef3 );
        assertDefaultOperatorTupleQueue( operatorReplica3, operatorDef3.inputPortCount(), MULTI_THREADED );
        assertEmptytOperatorKVStore( operatorReplica3 );
        assertBlockingTupleQueueDrainerPool( operatorReplica3 );
        assertNonCachedTuplesImplSupplier( operatorReplica3 );
    }

    @Test
    public void test_partitionedStatefulRegion_threePipeline_singleReplica_withStatelessOperatorFirst ()
    {
        final OperatorRuntimeSchemaBuilder operatorRuntimeSchemaBuilder0 = new OperatorRuntimeSchemaBuilder( 1, 1 );
        operatorRuntimeSchemaBuilder0.addInputField( 0, "field1", Integer.class ).addOutputField( 0, "field2", Integer.class );
        final OperatorRuntimeSchemaBuilder operatorRuntimeSchemaBuilder1 = new OperatorRuntimeSchemaBuilder( 1, 1 );
        operatorRuntimeSchemaBuilder1.addInputField( 0, "field2", Integer.class ).addOutputField( 0, "field2", Integer.class );
        final OperatorRuntimeSchemaBuilder operatorRuntimeSchemaBuilder2 = new OperatorRuntimeSchemaBuilder( 2, 1 );
        operatorRuntimeSchemaBuilder2.addInputField( 0, "field2", Integer.class )
                                     .addInputField( 1, "field2", Integer.class )
                                     .addOutputField( 0, "field3", Integer.class );
        final OperatorRuntimeSchemaBuilder operatorRuntimeSchemaBuilder3 = new OperatorRuntimeSchemaBuilder( 1, 1 );
        operatorRuntimeSchemaBuilder3.addInputField( 0, "field3", Integer.class ).addOutputField( 0, "field3", Integer.class );
        final OperatorDef operatorDef0 = OperatorDefBuilder.newInstance( "op0", StatefulOperatorWithSingleInputOutputPortCount.class )
                                                           .setExtendingSchema( operatorRuntimeSchemaBuilder0 )
                                                           .build();
        final OperatorDef operatorDef1 = OperatorDefBuilder.newInstance( "op1", StatelessOperatorWithSingleInputOutputPortCount.class )
                                                           .setExtendingSchema( operatorRuntimeSchemaBuilder1 )
                                                           .build();
        final OperatorDef operatorDef2 = OperatorDefBuilder.newInstance( "op2",
                                                                         PartitionedStatefulOperatorWithSingleInputOutputPortCount.class )
                                                           .setExtendingSchema( operatorRuntimeSchemaBuilder2 )
                                                           .setPartitionFieldNames( singletonList( "field2" ) )
                                                           .build();
        final OperatorDef operatorDef3 = OperatorDefBuilder.newInstance( "op3", StatelessOperatorWithSingleInputOutputPortCount.class )
                                                           .setExtendingSchema( operatorRuntimeSchemaBuilder3 )
                                                           .build();

        final FlowDef flow = new FlowDefBuilder().add( operatorDef0 )
                                                 .add( operatorDef1 )
                                                 .add( operatorDef2 )
                                                 .add( operatorDef3 )
                                                 .connect( "op0", "op1" )
                                                 .connect( "op1", "op2" )
                                                 .connect( "op2", "op3" )
                                                 .build();

        final List<RegionDef> regionDefs = regionDefFormer.createRegions( flow );
        final RegionDef regionDef = regionDefs.get( 1 );
        final RegionConfig regionConfig = new RegionConfig( regionDef, asList( 0, 1, 2 ), 1 );

        final Region region = regionManager.createRegion( flow, regionConfig );
        assertNotNull( region );
        final PipelineReplica[] pipelines = region.getReplicaPipelines( 0 );
        assertEquals( 3, pipelines.length );
        final PipelineReplica pipeline0 = pipelines[ 0 ];
        final PipelineReplica pipeline1 = pipelines[ 1 ];
        final PipelineReplica pipeline2 = pipelines[ 2 ];
        assertDefaultOperatorTupleQueue( pipeline0, operatorDef1.inputPortCount() );
        assertEmptySelfPipelinTupleQueue( pipeline0 );
        assertDefaultOperatorTupleQueue( pipeline1, operatorDef2.inputPortCount() );
        assertDefaultSelfPipelineTupleQueue( pipeline1 );
        assertDefaultOperatorTupleQueue( pipeline2, operatorDef3.inputPortCount() );
        assertEmptySelfPipelinTupleQueue( pipeline2 );

        assertEquals( new PipelineReplicaId( new PipelineId( 1, 0 ), 0 ), pipeline0.id() );
        assertEquals( new PipelineReplicaId( new PipelineId( 1, 1 ), 0 ), pipeline1.id() );
        assertEquals( new PipelineReplicaId( new PipelineId( 1, 2 ), 0 ), pipeline2.id() );
        assertEquals( 1, pipeline0.getOperatorCount() );
        assertEquals( 1, pipeline1.getOperatorCount() );
        assertEquals( 1, pipeline2.getOperatorCount() );

        final OperatorReplica operatorReplica1 = pipeline0.getOperator( 0 );
        assertOperatorDef( operatorReplica1, operatorDef1 );
        assertDefaultOperatorTupleQueue( operatorReplica1, operatorDef1.inputPortCount(), MULTI_THREADED );
        assertEmptytOperatorKVStore( operatorReplica1 );
        assertBlockingTupleQueueDrainerPool( operatorReplica1 );
        assertNonCachedTuplesImplSupplier( operatorReplica1 );

        final OperatorReplica operatorReplica2 = pipeline1.getOperator( 0 );
        assertOperatorDef( operatorReplica2, operatorDef2 );
        assertPartitionedOperatorTupleQueue( operatorReplica2 );
        assertPartitionedOperatorKVStore( operatorReplica2 );
        assertBlockingTupleQueueDrainerPool( operatorReplica2 );
        assertNonCachedTuplesImplSupplier( operatorReplica2 );

        final OperatorReplica operatorReplica3 = pipeline2.getOperator( 0 );
        assertOperatorDef( operatorReplica3, operatorDef3 );
        assertDefaultOperatorTupleQueue( operatorReplica3, operatorDef3.inputPortCount(), MULTI_THREADED );
        assertEmptytOperatorKVStore( operatorReplica3 );
        assertBlockingTupleQueueDrainerPool( operatorReplica3 );
        assertNonCachedTuplesImplSupplier( operatorReplica3 );
    }

    private void assertDefaultSelfPipelineTupleQueue ( final PipelineReplica pipeline )
    {
        assertTrue( pipeline.getSelfPipelineTupleQueue() instanceof DefaultOperatorTupleQueue );
    }


    private void assertEmptySelfPipelinTupleQueue ( final PipelineReplica pipeline )
    {
        assertTrue( pipeline.getSelfPipelineTupleQueue() instanceof EmptyOperatorTupleQueue );
    }

    private void assertEmptyOperatorTupleQueue ( final PipelineReplica pipeline, final int inputPortCount )
    {
        final OperatorTupleQueue pipelineTupleQueue = pipeline.getPipelineTupleQueue();
        assertTrue( pipelineTupleQueue instanceof EmptyOperatorTupleQueue );
        assertEquals( inputPortCount, pipelineTupleQueue.getInputPortCount() );
    }

    private void assertDefaultOperatorTupleQueue ( final PipelineReplica pipeline, final int inputPortCount )
    {
        final OperatorTupleQueue pipelineTupleQueue = pipeline.getPipelineTupleQueue();
        assertTrue( pipelineTupleQueue instanceof DefaultOperatorTupleQueue );
        assertEquals( MULTI_THREADED, ( (DefaultOperatorTupleQueue) pipelineTupleQueue ).getThreadingPreference() );
        assertEquals( inputPortCount, pipelineTupleQueue.getInputPortCount() );
    }

    private void assertOperatorDef ( final OperatorReplica operatorReplica, final OperatorDef operatorDef )
    {
        assertTrue( operatorReplica.getOperatorDef() == operatorDef );
    }

    private void assertEmptyOperatorTupleQueue ( final OperatorReplica operatorReplica )
    {
        final OperatorTupleQueue operatorTupleQueue = operatorReplica.getQueue();
        assertTrue( operatorTupleQueue instanceof EmptyOperatorTupleQueue );
        assertEquals( operatorReplica.getOperatorDef().id(), operatorTupleQueue.getOperatorId() );
    }

    private void assertDefaultOperatorTupleQueue ( final OperatorReplica operatorReplica,
                                                   final int inputPortCount,
                                                   final ThreadingPreference threadingPreference )
    {
        final OperatorTupleQueue operatorTupleQueue = operatorReplica.getQueue();
        assertTrue( operatorTupleQueue instanceof DefaultOperatorTupleQueue );
        assertEquals( operatorReplica.getOperatorDef().id(), operatorTupleQueue.getOperatorId() );
        assertEquals( threadingPreference, ( (DefaultOperatorTupleQueue) operatorTupleQueue ).getThreadingPreference() );
        assertEquals( inputPortCount, operatorTupleQueue.getInputPortCount() );
    }

    private void assertPartitionedOperatorTupleQueue ( final OperatorReplica operatorReplica )
    {
        final OperatorTupleQueue operatorTupleQueue = operatorReplica.getQueue();
        assertTrue( operatorTupleQueue instanceof PartitionedOperatorTupleQueue );
        assertEquals( operatorReplica.getOperatorDef().id(), operatorTupleQueue.getOperatorId() );
    }

    private void assertEmptytOperatorKVStore ( final OperatorReplica operatorReplica )
    {
        assertTrue( operatorReplica.getOperatorKvStore() instanceof EmptyOperatorKVStore );
    }

    private void assertDefaultOperatorKVStore ( final OperatorReplica operatorReplica )
    {
        final OperatorKVStore operatorKvStore = operatorReplica.getOperatorKvStore();
        assertTrue( operatorKvStore instanceof DefaultOperatorKVStore );
        assertEquals( operatorReplica.getOperatorDef().id(), operatorKvStore.getOperatorId() );
    }

    private void assertPartitionedOperatorKVStore ( final OperatorReplica operatorReplica )
    {
        final OperatorKVStore operatorKvStore = operatorReplica.getOperatorKvStore();
        assertTrue( operatorKvStore instanceof PartitionedOperatorKVStore );
        assertEquals( operatorReplica.getOperatorDef().id(), operatorKvStore.getOperatorId() );
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
    static class StatelessOperatorWithSingleInputOutputPortCount extends NopOperator
    {

    }


    @OperatorSpec( type = OperatorType.PARTITIONED_STATEFUL, inputPortCount = 2, outputPortCount = 1 )
    static class PartitionedStatefulOperatorWithSingleInputOutputPortCount extends NopOperator
    {

    }


    @OperatorSpec( type = OperatorType.STATEFUL, inputPortCount = 1, outputPortCount = 1 )
    static class StatefulOperatorWithSingleInputOutputPortCount extends NopOperator
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
