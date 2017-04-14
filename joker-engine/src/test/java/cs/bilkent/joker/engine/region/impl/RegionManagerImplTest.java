package cs.bilkent.joker.engine.region.impl;

import java.util.List;

import org.junit.Test;

import cs.bilkent.joker.engine.config.JokerConfig;
import cs.bilkent.joker.engine.config.ThreadingPreference;
import static cs.bilkent.joker.engine.config.ThreadingPreference.MULTI_THREADED;
import static cs.bilkent.joker.engine.config.ThreadingPreference.SINGLE_THREADED;
import cs.bilkent.joker.engine.flow.PipelineId;
import cs.bilkent.joker.engine.flow.RegionDef;
import cs.bilkent.joker.engine.flow.RegionExecutionPlan;
import cs.bilkent.joker.engine.kvstore.OperatorKVStore;
import cs.bilkent.joker.engine.kvstore.impl.DefaultOperatorKVStore;
import cs.bilkent.joker.engine.kvstore.impl.EmptyOperatorKVStore;
import cs.bilkent.joker.engine.kvstore.impl.OperatorKVStoreManagerImpl;
import cs.bilkent.joker.engine.kvstore.impl.PartitionedOperatorKVStore;
import cs.bilkent.joker.engine.metric.PipelineReplicaMeter;
import cs.bilkent.joker.engine.partition.PartitionKeyExtractorFactory;
import cs.bilkent.joker.engine.partition.PartitionService;
import cs.bilkent.joker.engine.partition.impl.PartitionKeyExtractorFactoryImpl;
import cs.bilkent.joker.engine.partition.impl.PartitionServiceImpl;
import cs.bilkent.joker.engine.pipeline.OperatorReplica;
import cs.bilkent.joker.engine.pipeline.PipelineReplica;
import cs.bilkent.joker.engine.pipeline.PipelineReplicaId;
import cs.bilkent.joker.engine.pipeline.impl.tuplesupplier.CachedTuplesImplSupplier;
import cs.bilkent.joker.engine.region.PipelineTransformer;
import cs.bilkent.joker.engine.region.Region;
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
import static cs.bilkent.joker.operator.spec.OperatorType.PARTITIONED_STATEFUL;
import static cs.bilkent.joker.operator.spec.OperatorType.STATEFUL;
import static cs.bilkent.joker.operator.spec.OperatorType.STATELESS;
import cs.bilkent.joker.test.AbstractJokerTest;
import cs.bilkent.joker.utils.Pair;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class RegionManagerImplTest extends AbstractJokerTest
{


    private final JokerConfig config = new JokerConfig();

    private final IdGenerator idGenerator = new IdGenerator();

    private final RegionDefFormerImpl regionDefFormer = new RegionDefFormerImpl( idGenerator );

    private final FlowDefOptimizerImpl flowOptimizer = new FlowDefOptimizerImpl( config, idGenerator );

    private final PartitionService partitionService = new PartitionServiceImpl( config );

    private final OperatorKVStoreManagerImpl operatorKVStoreManager = new OperatorKVStoreManagerImpl();

    private final OperatorTupleQueueManagerImpl operatorTupleQueueManager = new OperatorTupleQueueManagerImpl( config,
                                                                                                               new PartitionKeyExtractorFactoryImpl() );

    private final PipelineTransformer pipelineTransformer = new PipelineTransformerImpl( config, operatorTupleQueueManager );

    private final PartitionKeyExtractorFactory partitionKeyExtractorFactory = new PartitionKeyExtractorFactoryImpl();

    private final RegionManagerImpl regionManager = new RegionManagerImpl( config,
                                                                           partitionService,
                                                                           operatorKVStoreManager,
                                                                           operatorTupleQueueManager,
                                                                           pipelineTransformer,
                                                                           partitionKeyExtractorFactory );


    @Test
    public void test_statelessRegion_singlePipeline_singleReplica_noInputConnection ()
    {
        final FlowExample1 flowExample1 = new FlowExample1();

        final List<RegionDef> regionDefs = regionDefFormer.createRegions( flowExample1.flow );
        final RegionDef regionDef = regionDefs.get( 0 );
        final RegionExecutionPlan regionExecutionPlan = new RegionExecutionPlan( regionDef, singletonList( 0 ), 1 );

        final Region region = regionManager.createRegion( flowExample1.flow, regionExecutionPlan );
        assertNotNull( region );
        final PipelineReplica[] pipelines = region.getReplicaPipelines( 0 );
        assertEquals( 1, pipelines.length );
        final PipelineReplica pipeline = pipelines[ 0 ];
        assertEmptyOperatorTupleQueue( pipeline, flowExample1.operatorDef0.getInputPortCount() );

        assertEquals( new PipelineReplicaId( new PipelineId( regionDef.getRegionId(), 0 ), 0 ), pipeline.id() );
        assertEquals( 1, pipeline.getOperatorCount() );
        final OperatorReplica operatorReplica0 = pipeline.getOperator( 0 );
        assertOperatorDef( operatorReplica0, flowExample1.operatorDef0 );
        assertEmptyOperatorTupleQueue( operatorReplica0 );
        assertEmptyOperatorKVStore( operatorReplica0 );
        assertNonBlockingTupleQueueDrainerPool( operatorReplica0 );
        assertLastOperatorOutputSupplier( config, operatorReplica0 );

        assertPipelineReplicaMeter( pipeline );
    }

    @Test
    public void test_statelessRegion_singlePipeline_singleReplica_withInputConnection ()
    {
        final FlowExample2 flowExample2 = new FlowExample2();

        final List<RegionDef> regionDefs = regionDefFormer.createRegions( flowExample2.flow );
        final RegionDef regionDef = regionDefs.get( 1 );
        final RegionExecutionPlan regionExecutionPlan = new RegionExecutionPlan( regionDef, singletonList( 0 ), 1 );

        final Region region = regionManager.createRegion( flowExample2.flow, regionExecutionPlan );
        assertNotNull( region );
        final PipelineReplica[] pipelines = region.getReplicaPipelines( 0 );
        assertEquals( 1, pipelines.length );
        final PipelineReplica pipeline = pipelines[ 0 ];
        assertDefaultOperatorTupleQueue( pipeline, flowExample2.operatorDef1.getInputPortCount() );
        assertEmptySelfPipelineTupleQueue( pipeline );

        assertEquals( new PipelineReplicaId( new PipelineId( region.getRegionId(), 0 ), 0 ), pipeline.id() );
        assertEquals( 2, pipeline.getOperatorCount() );
        final OperatorReplica operatorReplica1 = pipeline.getOperator( 0 );
        final OperatorReplica operatorReplica2 = pipeline.getOperator( 1 );
        assertOperatorDef( operatorReplica1, flowExample2.operatorDef1 );
        assertOperatorDef( operatorReplica2, flowExample2.operatorDef2 );
        assertDefaultOperatorTupleQueue( operatorReplica1, flowExample2.operatorDef1.getInputPortCount(), MULTI_THREADED );
        assertDefaultOperatorTupleQueue( operatorReplica2, flowExample2.operatorDef2.getInputPortCount(), SINGLE_THREADED );
        assertEmptyOperatorKVStore( operatorReplica1 );
        assertEmptyOperatorKVStore( operatorReplica2 );
        assertBlockingTupleQueueDrainerPool( operatorReplica1 );
        assertNonBlockingTupleQueueDrainerPool( operatorReplica2 );
        assertCachedTuplesImplSupplier( operatorReplica1 );
        assertLastOperatorOutputSupplier( config, operatorReplica2 );

        assertPipelineReplicaMeter( pipeline );
    }

    @Test
    public void test_statefulRegion_singlePipeline_singleReplica ()
    {
        final FlowExample3 flowExample3 = new FlowExample3();

        final List<RegionDef> regionDefs = regionDefFormer.createRegions( flowExample3.flow );
        final RegionDef regionDef = regionDefs.get( 1 );
        final RegionExecutionPlan regionExecutionPlan = new RegionExecutionPlan( regionDef, singletonList( 0 ), 1 );

        final Region region = regionManager.createRegion( flowExample3.flow, regionExecutionPlan );
        assertNotNull( region );
        final PipelineReplica[] pipelines = region.getReplicaPipelines( 0 );
        assertEquals( 1, pipelines.length );
        final PipelineReplica pipeline = pipelines[ 0 ];
        assertDefaultOperatorTupleQueue( pipeline, flowExample3.operatorDef1.getInputPortCount() );
        assertEmptySelfPipelineTupleQueue( pipeline );

        assertEquals( new PipelineReplicaId( new PipelineId( regionDef.getRegionId(), 0 ), 0 ), pipeline.id() );
        assertEquals( 1, pipeline.getOperatorCount() );

        final OperatorReplica operatorReplica1 = pipeline.getOperator( 0 );
        assertOperatorDef( operatorReplica1, flowExample3.operatorDef1 );
        assertDefaultOperatorTupleQueue( operatorReplica1, flowExample3.operatorDef1.getInputPortCount(), MULTI_THREADED );
        assertDefaultOperatorKVStore( operatorReplica1 );
        assertBlockingTupleQueueDrainerPool( operatorReplica1 );
        assertLastOperatorOutputSupplier( config, operatorReplica1 );

        assertPipelineReplicaMeter( pipeline );
    }

    @Test
    public void test_statefulRegion_singlePipeline_singleReplica_withStatelessOperator ()
    {
        final FlowExample3 flowExample3 = new FlowExample3();

        final Pair<FlowDef, List<RegionDef>> result = flowOptimizer.optimize( flowExample3.flow,
                                                                              regionDefFormer.createRegions( flowExample3.flow ) );
        final FlowDef flow = result._1;
        final List<RegionDef> regionDefs = result._2;

        final RegionDef regionDef = regionDefs.get( 1 );
        final RegionExecutionPlan regionExecutionPlan = new RegionExecutionPlan( regionDef, singletonList( 0 ), 1 );

        final Region region = regionManager.createRegion( flow, regionExecutionPlan );
        assertNotNull( region );
        final PipelineReplica[] pipelines = region.getReplicaPipelines( 0 );
        assertEquals( 1, pipelines.length );
        final PipelineReplica pipeline = pipelines[ 0 ];
        assertDefaultOperatorTupleQueue( pipeline, flowExample3.operatorDef1.getInputPortCount() );
        assertEmptySelfPipelineTupleQueue( pipeline );

        assertEquals( 2, pipeline.getOperatorCount() );

        final OperatorReplica operator0 = pipeline.getOperator( 0 );
        assertOperatorDef( operator0, flowExample3.operatorDef1 );
        assertDefaultOperatorTupleQueue( operator0, flowExample3.operatorDef1.getInputPortCount(), MULTI_THREADED );
        assertDefaultOperatorKVStore( operator0 );
        assertBlockingTupleQueueDrainerPool( operator0 );
        assertCachedTuplesImplSupplier( operator0 );

        final OperatorReplica operator1 = pipeline.getOperator( 1 );
        assertOperatorDef( operator1, flowExample3.operatorDef2 );
        assertDefaultOperatorTupleQueue( operator1, flowExample3.operatorDef2.getInputPortCount(), SINGLE_THREADED );
        assertEmptyOperatorKVStore( operator1 );
        assertNonBlockingTupleQueueDrainerPool( operator1 );
        assertLastOperatorOutputSupplier( config, operator1 );

        assertPipelineReplicaMeter( pipeline );
    }

    @Test
    public void test_release_statefulRegion_singlePipeline_singleReplica ()
    {
        final FlowExample3 flowExample3 = new FlowExample3();

        final Pair<FlowDef, List<RegionDef>> result = flowOptimizer.optimize( flowExample3.flow,
                                                                              regionDefFormer.createRegions( flowExample3.flow ) );
        final FlowDef flow = result._1;
        final List<RegionDef> regionDefs = result._2;

        final RegionDef regionDef = regionDefs.get( 1 );
        final RegionExecutionPlan regionExecutionPlan = new RegionExecutionPlan( regionDef, singletonList( 0 ), 1 );

        final Region region = regionManager.createRegion( flow, regionExecutionPlan );

        assertNotNull( operatorTupleQueueManager.getDefaultOperatorTupleQueue( region.getRegionId(),
                                                                               0,
                                                                               flowExample3.operatorDef1.getId() ) );
        assertNotNull( operatorKVStoreManager.getDefaultOperatorKVStore( region.getRegionId(), flowExample3.operatorDef1.getId() ) );

        regionManager.releaseRegion( region.getRegionId() );

        assertNull( operatorTupleQueueManager.getDefaultOperatorTupleQueue( region.getRegionId(), 0, flowExample3.operatorDef1.getId() ) );
        assertNull( operatorKVStoreManager.getDefaultOperatorKVStore( region.getRegionId(), flowExample3.operatorDef1.getId() ) );
    }

    @Test
    public void test_release_statefulRegion_singlePipeline_singleReplica_withStatelessOperator ()
    {
        final FlowExample3 flowExample3 = new FlowExample3();

        List<RegionDef> r = regionDefFormer.createRegions( flowExample3.flow );
        Pair<FlowDef, List<RegionDef>> result = flowOptimizer.optimize( flowExample3.flow, r );
        final List<RegionDef> regionDefs = result._2;
        final RegionDef regionDef = regionDefs.get( 1 );
        final RegionExecutionPlan regionExecutionPlan = new RegionExecutionPlan( regionDef, singletonList( 0 ), 1 );

        final Region region = regionManager.createRegion( result._1, regionExecutionPlan );

        assertNotNull( operatorTupleQueueManager.getDefaultOperatorTupleQueue( region.getRegionId(),
                                                                               0,
                                                                               flowExample3.operatorDef1.getId() ) );
        assertNotNull( operatorKVStoreManager.getDefaultOperatorKVStore( region.getRegionId(), flowExample3.operatorDef1.getId() ) );

        assertNotNull( operatorTupleQueueManager.getDefaultOperatorTupleQueue( region.getRegionId(),
                                                                               0,
                                                                               flowExample3.operatorDef2.getId() ) );

        regionManager.releaseRegion( region.getRegionId() );

        assertNull( operatorTupleQueueManager.getDefaultOperatorTupleQueue( region.getRegionId(), 0, flowExample3.operatorDef1.getId() ) );
        assertNull( operatorKVStoreManager.getDefaultOperatorKVStore( region.getRegionId(), flowExample3.operatorDef1.getId() ) );
        assertNull( operatorTupleQueueManager.getDefaultOperatorTupleQueue( region.getRegionId(), 0, flowExample3.operatorDef2.getId() ) );
    }

    @Test
    public void test_partitionedStatefulRegion_singlePipeline_singleReplica ()
    {
        final FlowExample4 flowExample4 = new FlowExample4();

        final List<RegionDef> regionDefs = regionDefFormer.createRegions( flowExample4.flow );
        final RegionDef regionDef = regionDefs.get( 1 );
        final RegionExecutionPlan regionExecutionPlan = new RegionExecutionPlan( regionDef, singletonList( 0 ), 1 );

        final Region region = regionManager.createRegion( flowExample4.flow, regionExecutionPlan );
        assertNotNull( region );
        final PipelineReplica[] pipelines = region.getReplicaPipelines( 0 );
        assertEquals( 1, pipelines.length );
        final PipelineReplica pipeline = pipelines[ 0 ];
        assertDefaultOperatorTupleQueue( pipeline, flowExample4.operatorDef1.getInputPortCount() );
        assertDefaultSelfPipelineTupleQueue( pipeline );

        assertEquals( new PipelineReplicaId( new PipelineId( regionDef.getRegionId(), 0 ), 0 ), pipeline.id() );
        assertEquals( 1, pipeline.getOperatorCount() );

        final OperatorReplica operatorReplica1 = pipeline.getOperator( 0 );
        assertOperatorDef( operatorReplica1, flowExample4.operatorDef1 );
        assertPartitionedOperatorTupleQueue( operatorReplica1 );
        assertPartitionedOperatorKVStore( operatorReplica1 );
        assertNonBlockingTupleQueueDrainerPool( operatorReplica1 );
        assertLastOperatorOutputSupplier( config, operatorReplica1 );

        assertPipelineReplicaMeter( pipeline );
    }

    @Test
    public void test_release_partitionedStatefulRegion_singlePipeline_singleReplica ()
    {
        final FlowExample4 flowExample4 = new FlowExample4();

        final List<RegionDef> regionDefs = regionDefFormer.createRegions( flowExample4.flow );
        final RegionDef regionDef = regionDefs.get( 1 );
        final RegionExecutionPlan regionExecutionPlan = new RegionExecutionPlan( regionDef, singletonList( 0 ), 1 );

        final Region region = regionManager.createRegion( flowExample4.flow, regionExecutionPlan );

        final OperatorKVStore[] operatorKvStores = operatorKVStoreManager.getPartitionedOperatorKVStores( region.getRegionId(),
                                                                                                          flowExample4.operatorDef1.getId() );
        assertNotNull( operatorKvStores );
        assertEquals( 1, operatorKvStores.length );

        final OperatorTupleQueue[] operatorTupleQueues = operatorTupleQueueManager.getPartitionedOperatorTupleQueues( region.getRegionId(),
                                                                                                                      flowExample4.operatorDef1
                                                                                                                              .getId() );

        assertNotNull( operatorTupleQueues );
        assertEquals( 1, operatorTupleQueues.length );

        regionManager.releaseRegion( region.getRegionId() );

        assertNull( operatorKVStoreManager.getPartitionedOperatorKVStores( region.getRegionId(), flowExample4.operatorDef1.getId() ) );
        assertNull( operatorTupleQueueManager.getPartitionedOperatorTupleQueues( region.getRegionId(),
                                                                                 flowExample4.operatorDef1.getId() ) );
    }

    @Test
    public void test_partitionedStatefulRegion_singlePipeline_multiReplica ()
    {
        final FlowExample4 flowExample4 = new FlowExample4();

        final List<RegionDef> regionDefs = regionDefFormer.createRegions( flowExample4.flow );
        final RegionDef regionDef = regionDefs.get( 1 );
        final RegionExecutionPlan regionExecutionPlan = new RegionExecutionPlan( regionDef, singletonList( 0 ), 2 );

        final Region region = regionManager.createRegion( flowExample4.flow, regionExecutionPlan );
        assertNotNull( region );

        for ( int replicaIndex = 0; replicaIndex < regionExecutionPlan.getReplicaCount(); replicaIndex++ )
        {
            final PipelineReplica[] pipelines = region.getReplicaPipelines( replicaIndex );
            assertEquals( 1, pipelines.length );
            final PipelineReplica pipelineReplica = pipelines[ 0 ];
            assertDefaultOperatorTupleQueue( pipelineReplica, flowExample4.operatorDef1.getInputPortCount() );
            assertDefaultSelfPipelineTupleQueue( pipelineReplica );

            assertEquals( new PipelineReplicaId( new PipelineId( regionDef.getRegionId(), 0 ), replicaIndex ), pipelineReplica.id() );
            assertEquals( 1, pipelineReplica.getOperatorCount() );

            final OperatorReplica operatorReplica = pipelineReplica.getOperator( 0 );
            assertOperatorDef( operatorReplica, flowExample4.operatorDef1 );
            assertPartitionedOperatorTupleQueue( operatorReplica );
            assertPartitionedOperatorKVStore( operatorReplica );
            assertNonBlockingTupleQueueDrainerPool( operatorReplica );
            assertLastOperatorOutputSupplier( config, operatorReplica );

            assertPipelineReplicaMeter( pipelineReplica );
        }
    }

    @Test
    public void test_partitionedStatefulRegion_singlePipeline_singleReplica_withStatelessOperatorFirst ()
    {
        final FlowExample5 flowExample5 = new FlowExample5();

        final List<RegionDef> regionDefs = regionDefFormer.createRegions( flowExample5.flow );
        final RegionDef regionDef = regionDefs.get( 1 );
        final RegionExecutionPlan regionExecutionPlan = new RegionExecutionPlan( regionDef, singletonList( 0 ), 1 );

        final Region region = regionManager.createRegion( flowExample5.flow, regionExecutionPlan );
        assertNotNull( region );
        final PipelineReplica[] pipelines = region.getReplicaPipelines( 0 );
        assertEquals( 1, pipelines.length );
        final PipelineReplica pipeline = pipelines[ 0 ];
        assertDefaultOperatorTupleQueue( pipeline, flowExample5.operatorDef1.getInputPortCount() );
        assertEmptySelfPipelineTupleQueue( pipeline );

        assertEquals( new PipelineReplicaId( new PipelineId( regionDef.getRegionId(), 0 ), 0 ), pipeline.id() );
        assertEquals( 2, pipeline.getOperatorCount() );

        final OperatorReplica operatorReplica1 = pipeline.getOperator( 0 );
        assertOperatorDef( operatorReplica1, flowExample5.operatorDef1 );
        assertDefaultOperatorTupleQueue( operatorReplica1, flowExample5.operatorDef1.getInputPortCount(), MULTI_THREADED );
        assertEmptyOperatorKVStore( operatorReplica1 );
        assertBlockingTupleQueueDrainerPool( operatorReplica1 );
        assertCachedTuplesImplSupplier( operatorReplica1 );

        final OperatorReplica operatorReplica2 = pipeline.getOperator( 1 );
        assertOperatorDef( operatorReplica2, flowExample5.operatorDef2 );
        assertPartitionedOperatorTupleQueue( operatorReplica2 );
        assertPartitionedOperatorKVStore( operatorReplica2 );
        assertNonBlockingTupleQueueDrainerPool( operatorReplica2 );
        assertLastOperatorOutputSupplier( config, operatorReplica2 );

        assertPipelineReplicaMeter( pipeline );
    }

    @Test
    public void test_partitionedStatefulRegion_singlePipeline_multiReplica_withStatelessOperatorFirst ()
    {
        final FlowExample5 flowExample5 = new FlowExample5();

        final List<RegionDef> regionDefs = regionDefFormer.createRegions( flowExample5.flow );
        final RegionDef regionDef = regionDefs.get( 1 );
        final RegionExecutionPlan regionExecutionPlan = new RegionExecutionPlan( regionDef, singletonList( 0 ), 2 );

        final Region region = regionManager.createRegion( flowExample5.flow, regionExecutionPlan );
        assertNotNull( region );

        for ( int replicaIndex = 0; replicaIndex < regionExecutionPlan.getReplicaCount(); replicaIndex++ )
        {
            final PipelineReplica[] pipelinesReplica = region.getReplicaPipelines( replicaIndex );
            assertEquals( 1, pipelinesReplica.length );
            final PipelineReplica pipelineReplica0 = pipelinesReplica[ 0 ];
            assertDefaultOperatorTupleQueue( pipelineReplica0, flowExample5.operatorDef1.getInputPortCount() );
            assertEmptySelfPipelineTupleQueue( pipelineReplica0 );

            assertEquals( new PipelineReplicaId( new PipelineId( regionDef.getRegionId(), 0 ), replicaIndex ), pipelineReplica0.id() );
            assertEquals( 2, pipelineReplica0.getOperatorCount() );

            final OperatorReplica operatorReplica1 = pipelineReplica0.getOperator( 0 );
            assertOperatorDef( operatorReplica1, flowExample5.operatorDef1 );
            assertDefaultOperatorTupleQueue( operatorReplica1, flowExample5.operatorDef1.getInputPortCount(), MULTI_THREADED );
            assertEmptyOperatorKVStore( operatorReplica1 );
            assertBlockingTupleQueueDrainerPool( operatorReplica1 );
            assertCachedTuplesImplSupplier( operatorReplica1 );

            final OperatorReplica operatorReplica2 = pipelineReplica0.getOperator( 1 );
            assertOperatorDef( operatorReplica2, flowExample5.operatorDef2 );
            assertPartitionedOperatorTupleQueue( operatorReplica2 );
            assertPartitionedOperatorKVStore( operatorReplica2 );
            assertNonBlockingTupleQueueDrainerPool( operatorReplica2 );
            assertLastOperatorOutputSupplier( config, operatorReplica2 );

            assertPipelineReplicaMeter( pipelineReplica0 );
        }

        final PipelineReplica[] pipelinesReplica0 = region.getReplicaPipelines( 0 );
        final PipelineReplica[] pipelinesReplica1 = region.getReplicaPipelines( 1 );
        final PipelineReplica pipelineReplica0 = pipelinesReplica0[ 0 ];
        final PipelineReplica pipelineReplica1 = pipelinesReplica1[ 0 ];
        assertFalse( pipelineReplica0.getPipelineTupleQueue() == pipelineReplica1.getPipelineTupleQueue() );
    }

    @Test
    public void test_release_partitionedStatefulRegion_singlePipeline_multiReplica_withStatelessOperatorFirst ()
    {
        final FlowExample5 flowExample5 = new FlowExample5();

        final List<RegionDef> regionDefs = regionDefFormer.createRegions( flowExample5.flow );
        final RegionDef regionDef = regionDefs.get( 1 );
        final RegionExecutionPlan regionExecutionPlan = new RegionExecutionPlan( regionDef, singletonList( 0 ), 2 );

        final Region region = regionManager.createRegion( flowExample5.flow, regionExecutionPlan );

        assertNotNull( operatorTupleQueueManager.getDefaultOperatorTupleQueue( region.getRegionId(),
                                                                               0,
                                                                               flowExample5.operatorDef1.getId() ) );
        assertNotNull( operatorTupleQueueManager.getDefaultOperatorTupleQueue( region.getRegionId(),
                                                                               1,
                                                                               flowExample5.operatorDef1.getId() ) );
        final OperatorTupleQueue[] operatorTupleQueues = operatorTupleQueueManager.getPartitionedOperatorTupleQueues( region.getRegionId(),
                                                                                                                      flowExample5.operatorDef2
                                                                                                                              .getId() );
        assertNotNull( operatorTupleQueues );
        assertEquals( 2, operatorTupleQueues.length );

        final OperatorKVStore[] operatorKvStores = operatorKVStoreManager.getPartitionedOperatorKVStores( region.getRegionId(),
                                                                                                          flowExample5.operatorDef2.getId() );
        assertNotNull( operatorKvStores );
        assertEquals( 2, operatorKvStores.length );

        regionManager.releaseRegion( region.getRegionId() );

        assertNull( operatorTupleQueueManager.getDefaultOperatorTupleQueue( region.getRegionId(), 0, flowExample5.operatorDef1.getId() ) );
        assertNull( operatorTupleQueueManager.getDefaultOperatorTupleQueue( region.getRegionId(), 1, flowExample5.operatorDef1.getId() ) );
        assertNull( operatorKVStoreManager.getPartitionedOperatorKVStores( region.getRegionId(), flowExample5.operatorDef2.getId() ) );
    }

    @Test
    public void test_partitionedStatefulRegion_twoPipelines_singleReplica_withStatelessOperatorFirst ()
    {
        final FlowExample6 flowExample6 = new FlowExample6();

        final List<RegionDef> regionDefs = regionDefFormer.createRegions( flowExample6.flow );
        final RegionDef regionDef = regionDefs.get( 1 );
        final RegionExecutionPlan regionExecutionPlan = new RegionExecutionPlan( regionDef, asList( 0, 1 ), 1 );

        final Region region = regionManager.createRegion( flowExample6.flow, regionExecutionPlan );
        assertNotNull( region );
        final PipelineReplica[] pipelines = region.getReplicaPipelines( 0 );
        assertEquals( 2, pipelines.length );
        final PipelineReplica pipeline0 = pipelines[ 0 ];
        final PipelineReplica pipeline1 = pipelines[ 1 ];
        assertDefaultOperatorTupleQueue( pipeline0, flowExample6.operatorDef1.getInputPortCount() );
        assertEmptySelfPipelineTupleQueue( pipeline0 );
        assertDefaultOperatorTupleQueue( pipeline1, flowExample6.operatorDef2.getInputPortCount() );
        assertDefaultSelfPipelineTupleQueue( pipeline1 );

        assertEquals( new PipelineReplicaId( new PipelineId( regionDef.getRegionId(), 0 ), 0 ), pipeline0.id() );
        assertEquals( new PipelineReplicaId( new PipelineId( regionDef.getRegionId(), 1 ), 0 ), pipeline1.id() );
        assertEquals( 1, pipeline0.getOperatorCount() );
        assertEquals( 2, pipeline1.getOperatorCount() );

        final OperatorReplica operatorReplica1 = pipeline0.getOperator( 0 );
        assertOperatorDef( operatorReplica1, flowExample6.operatorDef1 );
        assertDefaultOperatorTupleQueue( operatorReplica1, flowExample6.operatorDef1.getInputPortCount(), MULTI_THREADED );
        assertEmptyOperatorKVStore( operatorReplica1 );
        assertBlockingTupleQueueDrainerPool( operatorReplica1 );
        assertLastOperatorOutputSupplier( config, operatorReplica1 );

        final OperatorReplica operatorReplica2 = pipeline1.getOperator( 0 );
        assertOperatorDef( operatorReplica2, flowExample6.operatorDef2 );
        assertPartitionedOperatorTupleQueue( operatorReplica2 );
        assertPartitionedOperatorKVStore( operatorReplica2 );
        assertNonBlockingTupleQueueDrainerPool( operatorReplica2 );
        assertCachedTuplesImplSupplier( operatorReplica2 );

        final OperatorReplica operatorReplica3 = pipeline1.getOperator( 1 );
        assertOperatorDef( operatorReplica3, flowExample6.operatorDef3 );
        assertDefaultOperatorTupleQueue( operatorReplica3, flowExample6.operatorDef3.getInputPortCount(), SINGLE_THREADED );
        assertEmptyOperatorKVStore( operatorReplica3 );
        assertNonBlockingTupleQueueDrainerPool( operatorReplica3 );
        assertLastOperatorOutputSupplier( config, operatorReplica3 );

        assertPipelineReplicaMeter( pipeline0 );
        assertPipelineReplicaMeter( pipeline1 );
        assertNotEquals( pipeline0.getMeter(), pipeline1.getMeter() );
    }

    @Test
    public void test_partitionedStatefulRegion_twoPipelines_singleReplica_bothPipelinesStartWithStatelessOperator ()
    {
        final FlowExample6 flowExample6 = new FlowExample6();

        final List<RegionDef> regionDefs = regionDefFormer.createRegions( flowExample6.flow );
        final RegionDef regionDef = regionDefs.get( 1 );
        final RegionExecutionPlan regionExecutionPlan = new RegionExecutionPlan( regionDef, asList( 0, 2 ), 1 );

        final Region region = regionManager.createRegion( flowExample6.flow, regionExecutionPlan );
        assertNotNull( region );
        final PipelineReplica[] pipelines = region.getReplicaPipelines( 0 );
        assertEquals( 2, pipelines.length );
        final PipelineReplica pipeline0 = pipelines[ 0 ];
        final PipelineReplica pipeline1 = pipelines[ 1 ];
        assertDefaultOperatorTupleQueue( pipeline0, flowExample6.operatorDef1.getInputPortCount() );
        assertEmptySelfPipelineTupleQueue( pipeline0 );
        assertDefaultOperatorTupleQueue( pipeline1, flowExample6.operatorDef3.getInputPortCount() );
        assertEmptySelfPipelineTupleQueue( pipeline0 );

        assertEquals( new PipelineReplicaId( new PipelineId( regionDef.getRegionId(), 0 ), 0 ), pipeline0.id() );
        assertEquals( new PipelineReplicaId( new PipelineId( regionDef.getRegionId(), 2 ), 0 ), pipeline1.id() );
        assertEquals( 2, pipeline0.getOperatorCount() );
        assertEquals( 1, pipeline1.getOperatorCount() );

        final OperatorReplica operatorReplica1 = pipeline0.getOperator( 0 );
        assertOperatorDef( operatorReplica1, flowExample6.operatorDef1 );
        assertDefaultOperatorTupleQueue( operatorReplica1, flowExample6.operatorDef1.getInputPortCount(), MULTI_THREADED );
        assertEmptyOperatorKVStore( operatorReplica1 );
        assertBlockingTupleQueueDrainerPool( operatorReplica1 );
        assertCachedTuplesImplSupplier( operatorReplica1 );

        final OperatorReplica operatorReplica2 = pipeline0.getOperator( 1 );
        assertOperatorDef( operatorReplica2, flowExample6.operatorDef2 );
        assertPartitionedOperatorTupleQueue( operatorReplica2 );
        assertPartitionedOperatorKVStore( operatorReplica2 );
        assertNonBlockingTupleQueueDrainerPool( operatorReplica2 );
        assertLastOperatorOutputSupplier( config, operatorReplica2 );

        final OperatorReplica operatorReplica3 = pipeline1.getOperator( 0 );
        assertOperatorDef( operatorReplica3, flowExample6.operatorDef3 );
        assertDefaultOperatorTupleQueue( operatorReplica3, flowExample6.operatorDef3.getInputPortCount(), MULTI_THREADED );
        assertEmptyOperatorKVStore( operatorReplica3 );
        assertBlockingTupleQueueDrainerPool( operatorReplica3 );
        assertLastOperatorOutputSupplier( config, operatorReplica3 );

        assertPipelineReplicaMeter( pipeline0 );
        assertPipelineReplicaMeter( pipeline1 );
        assertNotEquals( pipeline0.getMeter(), pipeline1.getMeter() );
    }

    @Test
    public void test_partitionedStatefulRegion_threePipeline_singleReplica_withStatelessOperatorFirst ()
    {
        final FlowExample6 flowExample6 = new FlowExample6();

        final List<RegionDef> regionDefs = regionDefFormer.createRegions( flowExample6.flow );
        final RegionDef regionDef = regionDefs.get( 1 );
        final RegionExecutionPlan regionExecutionPlan = new RegionExecutionPlan( regionDef, asList( 0, 1, 2 ), 1 );

        final Region region = regionManager.createRegion( flowExample6.flow, regionExecutionPlan );
        assertNotNull( region );
        final PipelineReplica[] pipelines = region.getReplicaPipelines( 0 );
        assertEquals( 3, pipelines.length );
        final PipelineReplica pipeline0 = pipelines[ 0 ];
        final PipelineReplica pipeline1 = pipelines[ 1 ];
        final PipelineReplica pipeline2 = pipelines[ 2 ];
        assertDefaultOperatorTupleQueue( pipeline0, flowExample6.operatorDef1.getInputPortCount() );
        assertEmptySelfPipelineTupleQueue( pipeline0 );
        assertDefaultOperatorTupleQueue( pipeline1, flowExample6.operatorDef2.getInputPortCount() );
        assertDefaultSelfPipelineTupleQueue( pipeline1 );
        assertDefaultOperatorTupleQueue( pipeline2, flowExample6.operatorDef3.getInputPortCount() );
        assertEmptySelfPipelineTupleQueue( pipeline2 );

        assertEquals( new PipelineReplicaId( new PipelineId( regionDef.getRegionId(), 0 ), 0 ), pipeline0.id() );
        assertEquals( new PipelineReplicaId( new PipelineId( regionDef.getRegionId(), 1 ), 0 ), pipeline1.id() );
        assertEquals( new PipelineReplicaId( new PipelineId( regionDef.getRegionId(), 2 ), 0 ), pipeline2.id() );
        assertEquals( 1, pipeline0.getOperatorCount() );
        assertEquals( 1, pipeline1.getOperatorCount() );
        assertEquals( 1, pipeline2.getOperatorCount() );

        final OperatorReplica operatorReplica1 = pipeline0.getOperator( 0 );
        assertOperatorDef( operatorReplica1, flowExample6.operatorDef1 );
        assertDefaultOperatorTupleQueue( operatorReplica1, flowExample6.operatorDef1.getInputPortCount(), MULTI_THREADED );
        assertEmptyOperatorKVStore( operatorReplica1 );
        assertBlockingTupleQueueDrainerPool( operatorReplica1 );
        assertLastOperatorOutputSupplier( config, operatorReplica1 );

        final OperatorReplica operatorReplica2 = pipeline1.getOperator( 0 );
        assertOperatorDef( operatorReplica2, flowExample6.operatorDef2 );
        assertPartitionedOperatorTupleQueue( operatorReplica2 );
        assertPartitionedOperatorKVStore( operatorReplica2 );
        assertNonBlockingTupleQueueDrainerPool( operatorReplica2 );
        assertLastOperatorOutputSupplier( config, operatorReplica2 );

        final OperatorReplica operatorReplica3 = pipeline2.getOperator( 0 );
        assertOperatorDef( operatorReplica3, flowExample6.operatorDef3 );
        assertDefaultOperatorTupleQueue( operatorReplica3, flowExample6.operatorDef3.getInputPortCount(), MULTI_THREADED );
        assertEmptyOperatorKVStore( operatorReplica3 );
        assertBlockingTupleQueueDrainerPool( operatorReplica3 );
        assertLastOperatorOutputSupplier( config, operatorReplica3 );

        assertPipelineReplicaMeter( pipeline0 );
        assertPipelineReplicaMeter( pipeline1 );
        assertPipelineReplicaMeter( pipeline2 );
        assertNotEquals( pipeline0.getMeter(), pipeline1.getMeter() );
        assertNotEquals( pipeline0.getMeter(), pipeline2.getMeter() );
        assertNotEquals( pipeline1.getMeter(), pipeline2.getMeter() );
    }

    static void assertPipelineReplicaMeter ( final PipelineReplica pipelineReplica )
    {
        final PipelineReplicaMeter pipelineReplicaMeter = pipelineReplica.getMeter();
        for ( OperatorReplica operatorReplica : pipelineReplica.getOperators() )
        {
            assertEquals( pipelineReplicaMeter, operatorReplica.getMeter() );
        }
    }

    static void assertDefaultSelfPipelineTupleQueue ( final PipelineReplica pipeline )
    {
        assertTrue( pipeline.getSelfPipelineTupleQueue() instanceof DefaultOperatorTupleQueue );
    }

    static void assertEmptySelfPipelineTupleQueue ( final PipelineReplica pipeline )
    {
        assertTrue( pipeline.getSelfPipelineTupleQueue() instanceof EmptyOperatorTupleQueue );
    }

    static void assertEmptyOperatorTupleQueue ( final PipelineReplica pipeline, final int inputPortCount )
    {
        final OperatorTupleQueue pipelineTupleQueue = pipeline.getPipelineTupleQueue();
        assertTrue( pipelineTupleQueue instanceof EmptyOperatorTupleQueue );
        assertEquals( inputPortCount, pipelineTupleQueue.getInputPortCount() );
    }

    static void assertDefaultOperatorTupleQueue ( final PipelineReplica pipeline, final int inputPortCount )
    {
        final OperatorTupleQueue pipelineTupleQueue = pipeline.getPipelineTupleQueue();
        assertTrue( pipelineTupleQueue instanceof DefaultOperatorTupleQueue );
        assertEquals( MULTI_THREADED, ( (DefaultOperatorTupleQueue) pipelineTupleQueue ).getThreadingPreference() );
        assertEquals( inputPortCount, pipelineTupleQueue.getInputPortCount() );
    }

    static void assertOperatorDef ( final OperatorReplica operatorReplica, final OperatorDef operatorDef )
    {
        assertTrue( operatorReplica.getOperatorDef() == operatorDef );
    }

    static void assertEmptyOperatorTupleQueue ( final OperatorReplica operatorReplica )
    {
        final OperatorTupleQueue operatorTupleQueue = operatorReplica.getQueue();
        assertTrue( operatorTupleQueue instanceof EmptyOperatorTupleQueue );
        assertEquals( operatorReplica.getOperatorDef().getId(), operatorTupleQueue.getOperatorId() );
    }

    static void assertDefaultOperatorTupleQueue ( final OperatorReplica operatorReplica,
                                                  final int inputPortCount,
                                                  final ThreadingPreference threadingPreference )
    {
        final OperatorTupleQueue operatorTupleQueue = operatorReplica.getQueue();
        assertTrue( operatorTupleQueue instanceof DefaultOperatorTupleQueue );
        assertEquals( threadingPreference, ( (DefaultOperatorTupleQueue) operatorTupleQueue ).getThreadingPreference() );
        assertEquals( inputPortCount, operatorTupleQueue.getInputPortCount() );
    }

    static void assertPartitionedOperatorTupleQueue ( final OperatorReplica operatorReplica )
    {
        final OperatorTupleQueue operatorTupleQueue = operatorReplica.getQueue();
        assertTrue( operatorTupleQueue instanceof PartitionedOperatorTupleQueue );
        assertEquals( operatorReplica.getOperatorDef().getId(), operatorTupleQueue.getOperatorId() );
    }

    static void assertEmptyOperatorKVStore ( final OperatorReplica operatorReplica )
    {
        assertTrue( operatorReplica.getOperatorKvStore() instanceof EmptyOperatorKVStore );
    }

    static void assertDefaultOperatorKVStore ( final OperatorReplica operatorReplica )
    {
        final OperatorKVStore operatorKvStore = operatorReplica.getOperatorKvStore();
        assertTrue( operatorKvStore instanceof DefaultOperatorKVStore );
        assertEquals( operatorReplica.getOperatorDef().getId(), operatorKvStore.getOperatorId() );
    }

    static void assertPartitionedOperatorKVStore ( final OperatorReplica operatorReplica )
    {
        final OperatorKVStore operatorKvStore = operatorReplica.getOperatorKvStore();
        assertTrue( operatorKvStore instanceof PartitionedOperatorKVStore );
        assertEquals( operatorReplica.getOperatorDef().getId(), operatorKvStore.getOperatorId() );
    }

    static void assertBlockingTupleQueueDrainerPool ( final OperatorReplica operatorReplica )
    {
        assertTrue( operatorReplica.getDrainerPool() instanceof BlockingTupleQueueDrainerPool );
    }

    static void assertNonBlockingTupleQueueDrainerPool ( final OperatorReplica operatorReplica )
    {
        assertTrue( operatorReplica.getDrainerPool() instanceof NonBlockingTupleQueueDrainerPool );
    }

    static void assertCachedTuplesImplSupplier ( final OperatorReplica operatorReplica )
    {
        assertTrue( operatorReplica.getOutputSupplier() instanceof CachedTuplesImplSupplier );
    }

    static void assertLastOperatorOutputSupplier ( final JokerConfig config, final OperatorReplica operatorReplica )
    {
        assertThat( operatorReplica.getOutputSupplier().getClass(),
                    equalTo( config.getRegionManagerConfig().getPipelineTailOperatorOutputSupplierClass() ) );
    }

    @OperatorSpec( type = STATELESS, inputPortCount = 1, outputPortCount = 1 )
    static class StatelessOperatorWithSingleInputOutputPort extends NopOperator
    {

    }


    @OperatorSpec( type = STATELESS, inputPortCount = 0, outputPortCount = 1 )
    static class StatelessOperatorWithZeroInputSingleOutputPort extends NopOperator
    {

    }


    @OperatorSpec( type = PARTITIONED_STATEFUL, inputPortCount = 2, outputPortCount = 1 )
    static class PartitionedStatefulOperatorWithSingleInputOutputPort extends NopOperator
    {

    }


    @OperatorSpec( type = STATEFUL, inputPortCount = 1, outputPortCount = 1 )
    static class StatefulOperatorWithSingleInputOutputPort extends NopOperator
    {

    }


    @OperatorSpec( type = STATEFUL, inputPortCount = 0, outputPortCount = 1 )
    static class StatefulOperatorWithZeroInputSingleOutputPort extends NopOperator
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


    public static class FlowExample1
    {

        final OperatorDef operatorDef0 = OperatorDefBuilder.newInstance( "op0", StatelessOperatorWithZeroInputSingleOutputPort.class )
                                                           .build();

        final FlowDef flow = new FlowDefBuilder().add( operatorDef0 ).build();
    }


    public static class FlowExample2
    {

        final Class<StatelessOperatorWithSingleInputOutputPort> operatorClazz = StatelessOperatorWithSingleInputOutputPort.class;

        final OperatorDef operatorDef0 = OperatorDefBuilder.newInstance( "op0", StatefulOperatorWithZeroInputSingleOutputPort.class )
                                                           .build();

        final OperatorDef operatorDef1 = OperatorDefBuilder.newInstance( "op1", operatorClazz ).build();

        final OperatorDef operatorDef2 = OperatorDefBuilder.newInstance( "op2", operatorClazz ).build();

        final FlowDef flow = new FlowDefBuilder().add( operatorDef0 )
                                                 .add( operatorDef1 )
                                                 .add( operatorDef2 )
                                                 .connect( "op0", "op1" )
                                                 .connect( "op1", "op2" )
                                                 .build();

    }


    public static class FlowExample3
    {

        final OperatorDef operatorDef0 = OperatorDefBuilder.newInstance( "op0", StatefulOperatorWithZeroInputSingleOutputPort.class )
                                                           .build();

        final OperatorDef operatorDef1 = OperatorDefBuilder.newInstance( "op1", StatefulOperatorWithSingleInputOutputPort.class ).build();

        final OperatorDef operatorDef2 = OperatorDefBuilder.newInstance( "op2", StatelessOperatorWithSingleInputOutputPort.class ).build();

        final FlowDef flow = new FlowDefBuilder().add( operatorDef0 )
                                                 .add( operatorDef1 )
                                                 .add( operatorDef2 )
                                                 .connect( "op0", "op1" )
                                                 .connect( "op1", "op2" )
                                                 .build();

    }


    public static class FlowExample4
    {

        final OperatorRuntimeSchemaBuilder operatorRuntimeSchemaBuilder0 = new OperatorRuntimeSchemaBuilder( 0, 1 );

        final OperatorRuntimeSchemaBuilder operatorRuntimeSchemaBuilder1 = new OperatorRuntimeSchemaBuilder( 2, 1 );

        final OperatorDef operatorDef0;

        final OperatorDef operatorDef1;

        final FlowDef flow;

        public FlowExample4 ()
        {
            operatorRuntimeSchemaBuilder0.addOutputField( 0, "field2", Integer.class );
            operatorRuntimeSchemaBuilder1.addInputField( 0, "field2", Integer.class )
                                         .addInputField( 1, "field2", Integer.class )
                                         .addOutputField( 0, "field3", Integer.class );
            operatorDef0 = OperatorDefBuilder.newInstance( "op0", StatefulOperatorWithZeroInputSingleOutputPort.class )
                                             .setExtendingSchema( operatorRuntimeSchemaBuilder0 )
                                             .build();
            operatorDef1 = OperatorDefBuilder.newInstance( "op1", PartitionedStatefulOperatorWithSingleInputOutputPort.class )
                                             .setExtendingSchema( operatorRuntimeSchemaBuilder1 )
                                             .setPartitionFieldNames( singletonList( "field2" ) )
                                             .build();

            flow = new FlowDefBuilder().add( operatorDef0 ).add( operatorDef1 ).connect( "op0", "op1" ).build();
        }

    }


    public static class FlowExample5
    {

        final OperatorRuntimeSchemaBuilder operatorRuntimeSchemaBuilder0 = new OperatorRuntimeSchemaBuilder( 0, 1 );

        final OperatorRuntimeSchemaBuilder operatorRuntimeSchemaBuilder1 = new OperatorRuntimeSchemaBuilder( 1, 1 );

        final OperatorRuntimeSchemaBuilder operatorRuntimeSchemaBuilder2 = new OperatorRuntimeSchemaBuilder( 2, 1 );

        final OperatorDef operatorDef0;

        final OperatorDef operatorDef1;

        final OperatorDef operatorDef2;

        final FlowDef flow;

        public FlowExample5 ()
        {
            operatorRuntimeSchemaBuilder0.addOutputField( 0, "field2", Integer.class );
            operatorRuntimeSchemaBuilder1.addInputField( 0, "field2", Integer.class ).addOutputField( 0, "field2", Integer.class );
            operatorRuntimeSchemaBuilder2.addInputField( 0, "field2", Integer.class )
                                         .addInputField( 1, "field2", Integer.class )
                                         .addOutputField( 0, "field3", Integer.class );
            operatorDef0 = OperatorDefBuilder.newInstance( "op0", StatefulOperatorWithZeroInputSingleOutputPort.class )
                                             .setExtendingSchema( operatorRuntimeSchemaBuilder0 )
                                             .build();
            operatorDef1 = OperatorDefBuilder.newInstance( "op1", StatelessOperatorWithSingleInputOutputPort.class )
                                             .setExtendingSchema( operatorRuntimeSchemaBuilder1 )
                                             .build();
            operatorDef2 = OperatorDefBuilder.newInstance( "op2", PartitionedStatefulOperatorWithSingleInputOutputPort.class )
                                             .setExtendingSchema( operatorRuntimeSchemaBuilder2 )
                                             .setPartitionFieldNames( singletonList( "field2" ) )
                                             .build();
            flow = new FlowDefBuilder().add( operatorDef0 )
                                       .add( operatorDef1 )
                                       .add( operatorDef2 )
                                       .connect( "op0", "op1" )
                                       .connect( "op1", "op2" )
                                       .build();
        }

    }


    public static class FlowExample6
    {

        final static String PARTITION_KEY_FIELD = "field2";

        final OperatorRuntimeSchemaBuilder operatorRuntimeSchemaBuilder0 = new OperatorRuntimeSchemaBuilder( 0, 1 );

        final OperatorRuntimeSchemaBuilder operatorRuntimeSchemaBuilder1 = new OperatorRuntimeSchemaBuilder( 1, 1 );

        final OperatorRuntimeSchemaBuilder operatorRuntimeSchemaBuilder2 = new OperatorRuntimeSchemaBuilder( 2, 1 );

        final OperatorRuntimeSchemaBuilder operatorRuntimeSchemaBuilder3 = new OperatorRuntimeSchemaBuilder( 1, 1 );

        final OperatorDef operatorDef0;

        final OperatorDef operatorDef1;

        final OperatorDef operatorDef2;

        final OperatorDef operatorDef3;

        final FlowDef flow;

        public FlowExample6 ()
        {
            operatorRuntimeSchemaBuilder0.addOutputField( 0, "field2", Integer.class );
            operatorRuntimeSchemaBuilder1.addInputField( 0, "field2", Integer.class ).addOutputField( 0, "field2", Integer.class );
            operatorRuntimeSchemaBuilder2.addInputField( 0, "field2", Integer.class )
                                         .addInputField( 1, "field2", Integer.class )
                                         .addOutputField( 0, "field2", Integer.class )
                                         .addOutputField( 0, "field3", Integer.class );
            operatorRuntimeSchemaBuilder3.addInputField( 0, "field2", Integer.class )
                                         .addInputField( 0, "field3", Integer.class )
                                         .addOutputField( 0, "field3", Integer.class );
            operatorDef0 = OperatorDefBuilder.newInstance( "op0", StatefulOperatorWithZeroInputSingleOutputPort.class )
                                             .setExtendingSchema( operatorRuntimeSchemaBuilder0 )
                                             .build();
            operatorDef1 = OperatorDefBuilder.newInstance( "op1", StatelessOperatorWithSingleInputOutputPort.class )
                                             .setExtendingSchema( operatorRuntimeSchemaBuilder1 )
                                             .build();
            operatorDef2 = OperatorDefBuilder.newInstance( "op2", PartitionedStatefulOperatorWithSingleInputOutputPort.class )
                                             .setExtendingSchema( operatorRuntimeSchemaBuilder2 )
                                             .setPartitionFieldNames( singletonList( PARTITION_KEY_FIELD ) )
                                             .build();
            operatorDef3 = OperatorDefBuilder.newInstance( "op3", StatelessOperatorWithSingleInputOutputPort.class )
                                             .setExtendingSchema( operatorRuntimeSchemaBuilder3 )
                                             .build();
            flow = new FlowDefBuilder().add( operatorDef0 )
                                       .add( operatorDef1 )
                                       .add( operatorDef2 )
                                       .add( operatorDef3 )
                                       .connect( "op0", "op1" )
                                       .connect( "op1", "op2" )
                                       .connect( "op2", "op3" )
                                       .build();
        }

        public FlowDef getFlow ()
        {
            return flow;
        }

    }

}
