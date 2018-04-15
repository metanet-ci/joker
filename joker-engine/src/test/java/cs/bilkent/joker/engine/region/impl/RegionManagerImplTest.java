package cs.bilkent.joker.engine.region.impl;

import java.util.List;

import org.junit.Test;

import cs.bilkent.joker.engine.config.JokerConfig;
import cs.bilkent.joker.engine.config.ThreadingPref;
import static cs.bilkent.joker.engine.config.ThreadingPref.MULTI_THREADED;
import static cs.bilkent.joker.engine.config.ThreadingPref.SINGLE_THREADED;
import cs.bilkent.joker.engine.flow.RegionDef;
import cs.bilkent.joker.engine.flow.RegionExecPlan;
import cs.bilkent.joker.engine.kvstore.OperatorKVStore;
import cs.bilkent.joker.engine.kvstore.impl.OperatorKVStoreManagerImpl;
import cs.bilkent.joker.engine.metric.PipelineReplicaMeter;
import cs.bilkent.joker.engine.partition.PartitionKeyExtractorFactory;
import cs.bilkent.joker.engine.partition.PartitionService;
import cs.bilkent.joker.engine.partition.impl.PartitionKeyExtractorFactoryImpl;
import cs.bilkent.joker.engine.partition.impl.PartitionServiceImpl;
import cs.bilkent.joker.engine.pipeline.OperatorReplica;
import cs.bilkent.joker.engine.pipeline.PipelineReplica;
import cs.bilkent.joker.engine.pipeline.PipelineReplicaId;
import cs.bilkent.joker.engine.pipeline.impl.invocation.FusedInvocationCtx;
import cs.bilkent.joker.engine.pipeline.impl.invocation.FusedPartitionedInvocationCtx;
import cs.bilkent.joker.engine.region.PipelineTransformer;
import cs.bilkent.joker.engine.region.Region;
import static cs.bilkent.joker.engine.region.Region.findFusionStartIndices;
import cs.bilkent.joker.engine.tuplequeue.OperatorQueue;
import cs.bilkent.joker.engine.tuplequeue.impl.OperatorQueueManagerImpl;
import cs.bilkent.joker.engine.tuplequeue.impl.drainer.pool.BlockingTupleQueueDrainerPool;
import cs.bilkent.joker.engine.tuplequeue.impl.drainer.pool.NonBlockingTupleQueueDrainerPool;
import cs.bilkent.joker.engine.tuplequeue.impl.operator.DefaultOperatorQueue;
import cs.bilkent.joker.engine.tuplequeue.impl.operator.EmptyOperatorQueue;
import cs.bilkent.joker.engine.tuplequeue.impl.operator.PartitionedOperatorQueue;
import cs.bilkent.joker.flow.FlowDef;
import cs.bilkent.joker.flow.FlowDefBuilder;
import cs.bilkent.joker.operator.InitCtx;
import cs.bilkent.joker.operator.InvocationCtx;
import cs.bilkent.joker.operator.Operator;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.OperatorDefBuilder;
import cs.bilkent.joker.operator.impl.DefaultInvocationCtx;
import cs.bilkent.joker.operator.scheduling.ScheduleWhenAvailable;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.AT_LEAST;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.AT_LEAST_BUT_SAME_ON_ALL_PORTS;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.EXACT;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.scheduleWhenTuplesAvailableOnAll;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.scheduleWhenTuplesAvailableOnAny;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.scheduleWhenTuplesAvailableOnDefaultPort;
import cs.bilkent.joker.operator.scheduling.SchedulingStrategy;
import cs.bilkent.joker.operator.schema.runtime.OperatorRuntimeSchemaBuilder;
import cs.bilkent.joker.operator.spec.OperatorSpec;
import static cs.bilkent.joker.operator.spec.OperatorType.PARTITIONED_STATEFUL;
import static cs.bilkent.joker.operator.spec.OperatorType.STATEFUL;
import static cs.bilkent.joker.operator.spec.OperatorType.STATELESS;
import cs.bilkent.joker.operator.utils.Pair;
import cs.bilkent.joker.test.AbstractJokerTest;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertArrayEquals;
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

    private final OperatorKVStoreManagerImpl kvStoreManager = new OperatorKVStoreManagerImpl();

    private final PartitionKeyExtractorFactory partitionKeyExtractorFactory = new PartitionKeyExtractorFactoryImpl();

    private final OperatorQueueManagerImpl tupleQueueManager = new OperatorQueueManagerImpl( config, partitionKeyExtractorFactory );

    private final PipelineTransformer pipelineTransformer = new PipelineTransformerImpl( config,
                                                                                         partitionService,
                                                                                         tupleQueueManager,
                                                                                         kvStoreManager,
                                                                                         partitionKeyExtractorFactory );

    private final RegionManagerImpl regionManager = new RegionManagerImpl( config, partitionService, kvStoreManager, tupleQueueManager,
                                                                           pipelineTransformer,
                                                                           partitionKeyExtractorFactory );


    @Test
    public void test_statelessRegion_singlePipeline_singleReplica_noInputConnection ()
    {
        final FlowExample1 ex = new FlowExample1();

        final List<RegionDef> regionDefs = regionDefFormer.createRegions( ex.flow );
        final RegionDef regionDef = regionDefs.get( 0 );
        final RegionExecPlan regionExecPlan = new RegionExecPlan( regionDef, singletonList( 0 ), 1 );

        final Region region = regionManager.createRegion( ex.flow, regionExecPlan );

        assertNotNull( region );
        final PipelineReplica[] pipelines = region.getReplicaPipelines( 0 );
        assertEquals( 1, pipelines.length );
        final PipelineReplica pipeline = pipelines[ 0 ];
        assertEmptyOperatorQueue( pipeline, ex.operatorDef0.getInputPortCount() );

        assertEquals( new PipelineReplicaId( regionDef.getRegionId(), 0, 0 ), pipeline.id() );
        assertEquals( 1, pipeline.getOperatorCount() );
        final OperatorReplica operatorReplica = pipeline.getOperatorReplica( 0 );
        assertOperatorDef( operatorReplica, 0, ex.operatorDef0 );
        assertEmptyOperatorQueue( operatorReplica );
        assertNonBlockingTupleQueueDrainerPool( operatorReplica );

        assertArrayEquals( new SchedulingStrategy[] { ScheduleWhenAvailable.INSTANCE }, region.getSchedulingStrategies() );
        assertArrayEquals( new int[] { 0 }, region.getFusionStartIndices() );

        assertPipelineReplicaMeter( pipeline );
    }

    @Test
    public void test_statelessRegion_singlePipeline_singleReplica_withInputConnection ()
    {
        final FlowExample2 ex = new FlowExample2();

        final List<RegionDef> regionDefs = regionDefFormer.createRegions( ex.flow );
        final RegionDef regionDef = regionDefs.get( 1 );
        final RegionExecPlan regionExecPlan = new RegionExecPlan( regionDef, singletonList( 0 ), 1 );

        final Region region = regionManager.createRegion( ex.flow, regionExecPlan );

        assertNotNull( region );
        final PipelineReplica[] pipelines = region.getReplicaPipelines( 0 );
        assertEquals( 1, pipelines.length );
        final PipelineReplica pipeline = pipelines[ 0 ];
        assertDefaultOperatorQueue( pipeline, ex.operatorDef1.getInputPortCount() );
        assertEmptyPipelineQueue( pipeline );

        assertEquals( new PipelineReplicaId( region.getRegionId(), 0, 0 ), pipeline.id() );
        assertEquals( 2, pipeline.getOperatorCount() );
        final OperatorReplica operatorReplica = pipeline.getOperatorReplica( 0 );
        assertOperatorDef( operatorReplica, 0, ex.operatorDef1 );
        assertOperatorDef( operatorReplica, 1, ex.operatorDef2 );
        assertDefaultOperatorQueue( operatorReplica, ex.operatorDef1.getInputPortCount(), MULTI_THREADED );
        assertBlockingTupleQueueDrainerPool( operatorReplica );
        assertTrue( operatorReplica.getInvocationCtx( 0 ) instanceof DefaultInvocationCtx );
        assertTrue( operatorReplica.getInvocationCtx( 1 ) instanceof FusedInvocationCtx );

        assertArrayEquals( new SchedulingStrategy[] { scheduleWhenTuplesAvailableOnDefaultPort( 1 ),
                                                      scheduleWhenTuplesAvailableOnDefaultPort( 1 ) }, region.getSchedulingStrategies() );
        assertArrayEquals( new int[] { 0 }, region.getFusionStartIndices() );

        assertPipelineReplicaMeter( pipeline );
    }

    @Test
    public void test_statefulRegion_singlePipeline_singleReplica ()
    {
        final FlowExample3 ex = new FlowExample3();

        final List<RegionDef> regionDefs = regionDefFormer.createRegions( ex.flow );
        final RegionDef regionDef = regionDefs.get( 1 );
        final RegionExecPlan regionExecPlan = new RegionExecPlan( regionDef, singletonList( 0 ), 1 );

        final Region region = regionManager.createRegion( ex.flow, regionExecPlan );

        assertNotNull( region );
        final PipelineReplica[] pipelines = region.getReplicaPipelines( 0 );
        assertEquals( 1, pipelines.length );
        final PipelineReplica pipeline = pipelines[ 0 ];
        assertDefaultOperatorQueue( pipeline, ex.operatorDef1.getInputPortCount() );
        assertEmptyPipelineQueue( pipeline );

        assertEquals( new PipelineReplicaId( regionDef.getRegionId(), 0, 0 ), pipeline.id() );
        assertEquals( 1, pipeline.getOperatorCount() );

        final OperatorReplica operatorReplica = pipeline.getOperatorReplica( 0 );
        assertOperatorDef( operatorReplica, 0, ex.operatorDef1 );
        assertDefaultOperatorQueue( operatorReplica, ex.operatorDef1.getInputPortCount(), MULTI_THREADED );
        assertBlockingTupleQueueDrainerPool( operatorReplica );
        assertNotNull( kvStoreManager.getDefaultKVStore( region.getRegionId(), ex.operatorDef1.getId() ) );

        assertArrayEquals( new SchedulingStrategy[] { scheduleWhenTuplesAvailableOnDefaultPort( 1 ) }, region.getSchedulingStrategies() );
        assertArrayEquals( new int[] { 0 }, region.getFusionStartIndices() );

        assertPipelineReplicaMeter( pipeline );

        regionManager.releaseRegion( region.getRegionId() );

        assertNull( tupleQueueManager.getDefaultQueue( region.getRegionId(), ex.operatorDef1.getId(), 0 ) );
        assertNull( kvStoreManager.getDefaultKVStore( region.getRegionId(), ex.operatorDef1.getId() ) );

    }

    @Test
    public void test_statefulRegion_singlePipeline_singleReplica_withFusedStatelessOperator ()
    {
        final FlowExample3 ex = new FlowExample3();

        final Pair<FlowDef, List<RegionDef>> result = flowOptimizer.optimize( ex.flow, regionDefFormer.createRegions( ex.flow ) );
        final FlowDef flow = result._1;
        final List<RegionDef> regionDefs = result._2;

        final RegionDef regionDef = regionDefs.get( 1 );
        final RegionExecPlan regionExecPlan = new RegionExecPlan( regionDef, singletonList( 0 ), 1 );

        final Region region = regionManager.createRegion( flow, regionExecPlan );

        assertNotNull( region );
        final PipelineReplica[] pipelines = region.getReplicaPipelines( 0 );
        assertEquals( 1, pipelines.length );
        final PipelineReplica pipeline = pipelines[ 0 ];
        assertDefaultOperatorQueue( pipeline, ex.operatorDef1.getInputPortCount() );
        assertEmptyPipelineQueue( pipeline );

        assertEquals( 2, pipeline.getOperatorCount() );

        final OperatorReplica operator = pipeline.getOperatorReplica( 0 );
        assertOperatorDef( operator, 0, ex.operatorDef1 );
        assertOperatorDef( operator, 1, ex.operatorDef2 );
        assertDefaultOperatorQueue( operator, ex.operatorDef1.getInputPortCount(), MULTI_THREADED );
        assertNull( tupleQueueManager.getDefaultQueue( region.getRegionId(), ex.operatorDef2.getId(), 0 ) );
        assertBlockingTupleQueueDrainerPool( operator );
        assertTrue( operator.getInvocationCtx( 0 ) instanceof DefaultInvocationCtx );
        assertTrue( operator.getInvocationCtx( 1 ) instanceof FusedInvocationCtx );

        assertArrayEquals( new SchedulingStrategy[] { scheduleWhenTuplesAvailableOnDefaultPort( 1 ),
                                                      scheduleWhenTuplesAvailableOnDefaultPort( 1 ) }, region.getSchedulingStrategies() );
        assertArrayEquals( new int[] { 0 }, region.getFusionStartIndices() );

        assertPipelineReplicaMeter( pipeline );

        regionManager.releaseRegion( region.getRegionId() );

        assertNull( tupleQueueManager.getDefaultQueue( region.getRegionId(), ex.operatorDef1.getId(), 0 ) );
        assertNull( kvStoreManager.getDefaultKVStore( region.getRegionId(), ex.operatorDef1.getId() ) );
    }

    @Test
    public void test_statefulRegion_singlePipeline_singleReplica_withNonFusibleStatefulOperator ()
    {
        final FlowExample7 ex = new FlowExample7();

        final Pair<FlowDef, List<RegionDef>> result = flowOptimizer.optimize( ex.flow, regionDefFormer.createRegions( ex.flow ) );
        final FlowDef flow = result._1;
        final List<RegionDef> regionDefs = result._2;

        final RegionDef regionDef = regionDefs.get( 1 );
        final RegionExecPlan regionExecPlan = new RegionExecPlan( regionDef, singletonList( 0 ), 1 );

        final Region region = regionManager.createRegion( flow, regionExecPlan );

        assertNotNull( region );
        final PipelineReplica[] pipelines = region.getReplicaPipelines( 0 );
        assertEquals( 1, pipelines.length );
        final PipelineReplica pipeline = pipelines[ 0 ];
        assertDefaultOperatorQueue( pipeline, ex.operatorDef1.getInputPortCount() );
        assertEmptyPipelineQueue( pipeline );

        assertEquals( 2, pipeline.getOperatorCount() );

        final OperatorReplica operatorReplica1 = pipeline.getOperatorReplica( 0 );
        assertOperatorDef( operatorReplica1, 0, ex.operatorDef1 );
        assertDefaultOperatorQueue( operatorReplica1, ex.operatorDef1.getInputPortCount(), MULTI_THREADED );
        assertBlockingTupleQueueDrainerPool( operatorReplica1 );
        assertNotNull( kvStoreManager.getDefaultKVStore( region.getRegionId(), ex.operatorDef1.getId() ) );
        assertTrue( operatorReplica1.getInvocationCtx( 0 ) instanceof DefaultInvocationCtx );

        final OperatorReplica operatorReplica2 = pipeline.getOperatorReplica( 1 );
        assertOperatorDef( operatorReplica2, 0, ex.operatorDef2 );
        assertDefaultOperatorQueue( operatorReplica2, ex.operatorDef1.getInputPortCount(), SINGLE_THREADED );
        assertNonBlockingTupleQueueDrainerPool( operatorReplica2 );
        assertNotNull( kvStoreManager.getDefaultKVStore( region.getRegionId(), ex.operatorDef2.getId() ) );
        assertTrue( operatorReplica2.getInvocationCtx( 0 ) instanceof DefaultInvocationCtx );

        assertArrayEquals( new SchedulingStrategy[] { scheduleWhenTuplesAvailableOnDefaultPort( 1 ),
                                                      scheduleWhenTuplesAvailableOnDefaultPort( 2 ) }, region.getSchedulingStrategies() );
        assertArrayEquals( new int[] { 0, 1 }, region.getFusionStartIndices() );

        assertPipelineReplicaMeter( pipeline );

        regionManager.releaseRegion( region.getRegionId() );

        assertNull( tupleQueueManager.getDefaultQueue( region.getRegionId(), ex.operatorDef1.getId(), 0 ) );
        assertNull( tupleQueueManager.getDefaultQueue( region.getRegionId(), ex.operatorDef2.getId(), 0 ) );
        assertNull( kvStoreManager.getDefaultKVStore( region.getRegionId(), ex.operatorDef1.getId() ) );
        assertNull( kvStoreManager.getDefaultKVStore( region.getRegionId(), ex.operatorDef2.getId() ) );
    }

    @Test
    public void test_partitionedStatefulRegion_singlePipeline_singleReplica ()
    {
        final FlowExample4 ex = new FlowExample4();

        final List<RegionDef> regionDefs = regionDefFormer.createRegions( ex.flow );
        final RegionDef regionDef = regionDefs.get( 1 );
        final RegionExecPlan regionExecPlan = new RegionExecPlan( regionDef, singletonList( 0 ), 1 );

        final Region region = regionManager.createRegion( ex.flow, regionExecPlan );

        assertNotNull( region );
        final PipelineReplica[] pipelines = region.getReplicaPipelines( 0 );
        assertEquals( 1, pipelines.length );
        final PipelineReplica pipeline = pipelines[ 0 ];
        assertDefaultOperatorQueue( pipeline, ex.operatorDef1.getInputPortCount() );
        assertDefaultPipelineQueue( pipeline );

        assertEquals( new PipelineReplicaId( regionDef.getRegionId(), 0, 0 ), pipeline.id() );
        assertEquals( 1, pipeline.getOperatorCount() );

        final OperatorReplica operatorReplica1 = pipeline.getOperatorReplica( 0 );
        assertOperatorDef( operatorReplica1, 0, ex.operatorDef1 );
        assertPartitionedOperatorQueue( operatorReplica1 );
        assertNotNull( kvStoreManager.getPartitionedKVStores( region.getRegionId(), ex.operatorDef1.getId() ) );
        assertNonBlockingTupleQueueDrainerPool( operatorReplica1 );

        assertArrayEquals( new SchedulingStrategy[] { scheduleWhenTuplesAvailableOnDefaultPort( 1 ) }, region.getSchedulingStrategies() );
        assertArrayEquals( new int[] { 0 }, region.getFusionStartIndices() );

        assertPipelineReplicaMeter( pipeline );

        regionManager.releaseRegion( region.getRegionId() );

        assertNull( kvStoreManager.getPartitionedKVStores( region.getRegionId(), ex.operatorDef1.getId() ) );
        assertNull( tupleQueueManager.getPartitionedQueues( region.getRegionId(), ex.operatorDef1.getId() ) );
    }

    @Test
    public void test_partitionedStatefulRegion_singlePipeline_multiReplica ()
    {
        final FlowExample4 ex = new FlowExample4();

        final List<RegionDef> regionDefs = regionDefFormer.createRegions( ex.flow );
        final RegionDef regionDef = regionDefs.get( 1 );
        final RegionExecPlan regionExecPlan = new RegionExecPlan( regionDef, singletonList( 0 ), 2 );

        final Region region = regionManager.createRegion( ex.flow, regionExecPlan );

        assertNotNull( region );

        for ( int replicaIndex = 0; replicaIndex < regionExecPlan.getReplicaCount(); replicaIndex++ )
        {
            final PipelineReplica[] pipelines = region.getReplicaPipelines( replicaIndex );
            assertEquals( 1, pipelines.length );
            final PipelineReplica pipelineReplica = pipelines[ 0 ];
            assertDefaultOperatorQueue( pipelineReplica, ex.operatorDef1.getInputPortCount() );
            assertDefaultPipelineQueue( pipelineReplica );

            assertEquals( new PipelineReplicaId( regionDef.getRegionId(), 0, replicaIndex ), pipelineReplica.id() );
            assertEquals( 1, pipelineReplica.getOperatorCount() );

            final OperatorReplica operatorReplica = pipelineReplica.getOperatorReplica( 0 );
            assertOperatorDef( operatorReplica, 0, ex.operatorDef1 );
            assertPartitionedOperatorQueue( operatorReplica );
            assertNonBlockingTupleQueueDrainerPool( operatorReplica );

            assertPipelineReplicaMeter( pipelineReplica );
        }

        final OperatorKVStore[] kvStores = kvStoreManager.getPartitionedKVStores( region.getRegionId(), ex.operatorDef1.getId() );
        assertNotNull( kvStores );
        assertEquals( 2, kvStores.length );

        regionManager.releaseRegion( region.getRegionId() );

        assertNull( kvStoreManager.getPartitionedKVStores( region.getRegionId(), ex.operatorDef1.getId() ) );
        assertNull( tupleQueueManager.getPartitionedQueues( region.getRegionId(), ex.operatorDef1.getId() ) );
    }

    @Test
    public void test_partitionedStatefulRegion_singlePipeline_singleReplica_withStatelessOperatorFirst ()
    {
        final FlowExample5 ex = new FlowExample5();

        final List<RegionDef> regionDefs = regionDefFormer.createRegions( ex.flow );
        final RegionDef regionDef = regionDefs.get( 1 );
        final RegionExecPlan regionExecPlan = new RegionExecPlan( regionDef, singletonList( 0 ), 1 );

        final Region region = regionManager.createRegion( ex.flow, regionExecPlan );

        assertNotNull( region );
        final PipelineReplica[] pipelines = region.getReplicaPipelines( 0 );
        assertEquals( 1, pipelines.length );
        final PipelineReplica pipeline = pipelines[ 0 ];
        assertDefaultOperatorQueue( pipeline, ex.operatorDef1.getInputPortCount() );
        assertEmptyPipelineQueue( pipeline );

        assertEquals( new PipelineReplicaId( regionDef.getRegionId(), 0, 0 ), pipeline.id() );
        assertEquals( 2, pipeline.getOperatorCount() );

        final OperatorReplica operatorReplica = pipeline.getOperatorReplica( 0 );
        assertOperatorDef( operatorReplica, 0, ex.operatorDef1 );
        assertDefaultOperatorQueue( operatorReplica, ex.operatorDef1.getInputPortCount(), MULTI_THREADED );
        assertNull( kvStoreManager.getDefaultKVStore( region.getRegionId(), ex.operatorDef1.getId() ) );
        assertNull( kvStoreManager.getPartitionedKVStores( region.getRegionId(), ex.operatorDef1.getId() ) );
        assertBlockingTupleQueueDrainerPool( operatorReplica );
        assertOperatorDef( operatorReplica, 1, ex.operatorDef2 );
        assertNull( tupleQueueManager.getPartitionedQueues( region.getRegionId(), ex.operatorDef2.getId() ) );
        assertNotNull( kvStoreManager.getPartitionedKVStores( region.getRegionId(), ex.operatorDef2.getId() ) );

        assertTrue( operatorReplica.getInvocationCtx( 0 ) instanceof DefaultInvocationCtx );
        assertTrue( operatorReplica.getInvocationCtx( 1 ) instanceof FusedPartitionedInvocationCtx );

        assertArrayEquals( new SchedulingStrategy[] { scheduleWhenTuplesAvailableOnDefaultPort( 1 ),
                                                      scheduleWhenTuplesAvailableOnDefaultPort( 1 ) }, region.getSchedulingStrategies() );
        assertArrayEquals( new int[] { 0 }, region.getFusionStartIndices() );

        assertPipelineReplicaMeter( pipeline );

        regionManager.releaseRegion( region.getRegionId() );

        assertNull( tupleQueueManager.getDefaultQueue( region.getRegionId(), ex.operatorDef1.getId(), 0 ) );
        assertNull( kvStoreManager.getPartitionedKVStores( region.getRegionId(), ex.operatorDef2.getId() ) );
    }

    @Test
    public void test_partitionedStatefulRegion_singlePipeline_multiReplica_withStatelessOperatorFirst ()
    {
        final FlowExample5 ex = new FlowExample5();

        final List<RegionDef> regionDefs = regionDefFormer.createRegions( ex.flow );
        final RegionDef regionDef = regionDefs.get( 1 );
        final RegionExecPlan regionExecPlan = new RegionExecPlan( regionDef, singletonList( 0 ), 2 );

        final Region region = regionManager.createRegion( ex.flow, regionExecPlan );

        assertNotNull( region );

        for ( int replicaIndex = 0; replicaIndex < regionExecPlan.getReplicaCount(); replicaIndex++ )
        {
            final PipelineReplica[] pipelinesReplica = region.getReplicaPipelines( replicaIndex );
            assertEquals( 1, pipelinesReplica.length );
            final PipelineReplica pipelineReplica0 = pipelinesReplica[ 0 ];
            assertDefaultOperatorQueue( pipelineReplica0, ex.operatorDef1.getInputPortCount() );
            assertEmptyPipelineQueue( pipelineReplica0 );

            assertEquals( new PipelineReplicaId( regionDef.getRegionId(), 0, replicaIndex ), pipelineReplica0.id() );
            assertEquals( 2, pipelineReplica0.getOperatorCount() );

            final OperatorReplica operatorReplica = pipelineReplica0.getOperatorReplica( 0 );

            assertOperatorDef( operatorReplica, 0, ex.operatorDef1 );
            assertDefaultOperatorQueue( operatorReplica, ex.operatorDef1.getInputPortCount(), MULTI_THREADED );
            assertNull( kvStoreManager.getDefaultKVStore( region.getRegionId(), ex.operatorDef1.getId() ) );
            assertNull( kvStoreManager.getPartitionedKVStores( region.getRegionId(), ex.operatorDef1.getId() ) );
            assertBlockingTupleQueueDrainerPool( operatorReplica );

            assertOperatorDef( operatorReplica, 1, ex.operatorDef2 );

            assertTrue( operatorReplica.getInvocationCtx( 0 ) instanceof DefaultInvocationCtx );
            assertTrue( operatorReplica.getInvocationCtx( 1 ) instanceof FusedPartitionedInvocationCtx );

            assertPipelineReplicaMeter( pipelineReplica0 );
        }

        assertNull( tupleQueueManager.getPartitionedQueues( region.getRegionId(), ex.operatorDef2.getId() ) );
        assertNotNull( kvStoreManager.getPartitionedKVStores( region.getRegionId(), ex.operatorDef2.getId() ) );

        final PipelineReplica[] pipelinesReplica0 = region.getReplicaPipelines( 0 );
        final PipelineReplica[] pipelinesReplica1 = region.getReplicaPipelines( 1 );
        final PipelineReplica pipelineReplica0 = pipelinesReplica0[ 0 ];
        final PipelineReplica pipelineReplica1 = pipelinesReplica1[ 0 ];
        assertFalse( pipelineReplica0.getEffectiveQueue() == pipelineReplica1.getEffectiveQueue() );

        regionManager.releaseRegion( region.getRegionId() );

        assertNull( tupleQueueManager.getDefaultQueue( region.getRegionId(), ex.operatorDef1.getId(), 0 ) );
        assertNull( tupleQueueManager.getDefaultQueue( region.getRegionId(), ex.operatorDef1.getId(), 1 ) );
        assertNull( kvStoreManager.getPartitionedKVStores( region.getRegionId(), ex.operatorDef2.getId() ) );
    }

    @Test
    public void test_partitionedStatefulRegion_twoPipelines_singleReplica_withStatelessOperatorFirst ()
    {
        final FlowExample6 ex = new FlowExample6();

        final List<RegionDef> regionDefs = regionDefFormer.createRegions( ex.flow );
        final RegionDef regionDef = regionDefs.get( 1 );
        final RegionExecPlan regionExecPlan = new RegionExecPlan( regionDef, asList( 0, 1 ), 1 );

        final Region region = regionManager.createRegion( ex.flow, regionExecPlan );

        assertNotNull( region );
        final PipelineReplica[] pipelines = region.getReplicaPipelines( 0 );
        assertEquals( 2, pipelines.length );
        final PipelineReplica pipeline0 = pipelines[ 0 ];
        final PipelineReplica pipeline1 = pipelines[ 1 ];
        assertDefaultOperatorQueue( pipeline0, ex.operatorDef1.getInputPortCount() );
        assertEmptyPipelineQueue( pipeline0 );
        assertDefaultOperatorQueue( pipeline1, ex.operatorDef2.getInputPortCount() );
        assertDefaultPipelineQueue( pipeline1 );

        assertEquals( new PipelineReplicaId( regionDef.getRegionId(), 0, 0 ), pipeline0.id() );
        assertEquals( new PipelineReplicaId( regionDef.getRegionId(), 1, 0 ), pipeline1.id() );
        assertEquals( 1, pipeline0.getOperatorCount() );
        assertEquals( 2, pipeline1.getOperatorCount() );

        final OperatorReplica operatorReplica1 = pipeline0.getOperatorReplica( 0 );
        assertOperatorDef( operatorReplica1, 0, ex.operatorDef1 );
        assertDefaultOperatorQueue( operatorReplica1, ex.operatorDef1.getInputPortCount(), MULTI_THREADED );
        assertNull( kvStoreManager.getDefaultKVStore( region.getRegionId(), ex.operatorDef1.getId() ) );
        assertNull( kvStoreManager.getPartitionedKVStores( region.getRegionId(), ex.operatorDef1.getId() ) );
        assertBlockingTupleQueueDrainerPool( operatorReplica1 );
        assertTrue( operatorReplica1.getInvocationCtx( 0 ) instanceof DefaultInvocationCtx );

        final OperatorReplica operatorReplica2 = pipeline1.getOperatorReplica( 0 );
        assertOperatorDef( operatorReplica2, 0, ex.operatorDef2 );
        assertPartitionedOperatorQueue( operatorReplica2 );

        assertNotNull( kvStoreManager.getPartitionedKVStores( region.getRegionId(), ex.operatorDef2.getId() ) );
        assertNonBlockingTupleQueueDrainerPool( operatorReplica2 );

        assertOperatorDef( operatorReplica2, 1, ex.operatorDef3 );
        assertNull( kvStoreManager.getDefaultKVStore( region.getRegionId(), ex.operatorDef3.getId() ) );
        assertNull( kvStoreManager.getPartitionedKVStores( region.getRegionId(), ex.operatorDef3.getId() ) );

        assertTrue( operatorReplica2.getInvocationCtx( 0 ) instanceof DefaultInvocationCtx );
        assertTrue( operatorReplica2.getInvocationCtx( 1 ) instanceof FusedInvocationCtx );

        assertArrayEquals( new SchedulingStrategy[] { scheduleWhenTuplesAvailableOnDefaultPort( 1 ),
                                                      scheduleWhenTuplesAvailableOnDefaultPort( 1 ),
                                                      scheduleWhenTuplesAvailableOnDefaultPort( 1 ) }, region.getSchedulingStrategies() );
        assertArrayEquals( new int[] { 0 }, region.getFusionStartIndices() );

        assertPipelineReplicaMeter( pipeline0 );
        assertPipelineReplicaMeter( pipeline1 );
        assertNotEquals( pipeline0.getMeter(), pipeline1.getMeter() );

        regionManager.releaseRegion( region.getRegionId() );

        assertNull( tupleQueueManager.getDefaultQueue( region.getRegionId(), ex.operatorDef1.getId(), 0 ) );
        assertNull( tupleQueueManager.getPartitionedQueues( region.getRegionId(), ex.operatorDef2.getId() ) );
        assertNull( kvStoreManager.getPartitionedKVStores( region.getRegionId(), ex.operatorDef2.getId() ) );
    }

    @Test
    public void test_partitionedStatefulRegion_twoPipelines_singleReplica_bothPipelinesStartWithStatelessOperator ()
    {
        final FlowExample6 ex = new FlowExample6();

        final List<RegionDef> regionDefs = regionDefFormer.createRegions( ex.flow );
        final RegionDef regionDef = regionDefs.get( 1 );
        final RegionExecPlan regionExecPlan = new RegionExecPlan( regionDef, asList( 0, 2 ), 1 );

        final Region region = regionManager.createRegion( ex.flow, regionExecPlan );

        assertNotNull( region );
        final PipelineReplica[] pipelines = region.getReplicaPipelines( 0 );
        assertEquals( 2, pipelines.length );
        final PipelineReplica pipeline0 = pipelines[ 0 ];
        final PipelineReplica pipeline1 = pipelines[ 1 ];
        assertDefaultOperatorQueue( pipeline0, ex.operatorDef1.getInputPortCount() );
        assertEmptyPipelineQueue( pipeline0 );
        assertDefaultOperatorQueue( pipeline1, ex.operatorDef3.getInputPortCount() );
        assertEmptyPipelineQueue( pipeline0 );

        assertEquals( new PipelineReplicaId( regionDef.getRegionId(), 0, 0 ), pipeline0.id() );
        assertEquals( new PipelineReplicaId( regionDef.getRegionId(), 2, 0 ), pipeline1.id() );
        assertEquals( 2, pipeline0.getOperatorCount() );
        assertEquals( 1, pipeline0.getOperatorReplicaCount() );
        assertEquals( 1, pipeline1.getOperatorCount() );

        final OperatorReplica operatorReplica1 = pipeline0.getOperatorReplica( 0 );
        assertOperatorDef( operatorReplica1, 0, ex.operatorDef1 );
        assertDefaultOperatorQueue( operatorReplica1, ex.operatorDef1.getInputPortCount(), MULTI_THREADED );
        assertNull( kvStoreManager.getDefaultKVStore( region.getRegionId(), ex.operatorDef1.getId() ) );
        assertBlockingTupleQueueDrainerPool( operatorReplica1 );

        assertOperatorDef( operatorReplica1, 1, ex.operatorDef2 );
        assertNull( tupleQueueManager.getPartitionedQueues( region.getRegionId(), ex.operatorDef2.getId() ) );
        assertNotNull( kvStoreManager.getPartitionedKVStores( region.getRegionId(), ex.operatorDef2.getId() ) );

        assertTrue( operatorReplica1.getInvocationCtx( 0 ) instanceof DefaultInvocationCtx );
        assertTrue( operatorReplica1.getInvocationCtx( 1 ) instanceof FusedPartitionedInvocationCtx );

        final OperatorReplica operatorReplica2 = pipeline1.getOperatorReplica( 0 );
        assertOperatorDef( operatorReplica2, 0, ex.operatorDef3 );
        assertDefaultOperatorQueue( operatorReplica2, ex.operatorDef3.getInputPortCount(), MULTI_THREADED );
        assertNull( kvStoreManager.getDefaultKVStore( region.getRegionId(), ex.operatorDef3.getId() ) );
        assertBlockingTupleQueueDrainerPool( operatorReplica2 );

        assertPipelineReplicaMeter( pipeline0 );
        assertPipelineReplicaMeter( pipeline1 );
        assertNotEquals( pipeline0.getMeter(), pipeline1.getMeter() );
    }

    @Test
    public void test_partitionedStatefulRegion_threePipeline_singleReplica_withStatelessOperatorFirst ()
    {
        final FlowExample6 ex = new FlowExample6();

        final List<RegionDef> regionDefs = regionDefFormer.createRegions( ex.flow );
        final RegionDef regionDef = regionDefs.get( 1 );
        final RegionExecPlan regionExecPlan = new RegionExecPlan( regionDef, asList( 0, 1, 2 ), 1 );

        final Region region = regionManager.createRegion( ex.flow, regionExecPlan );

        assertNotNull( region );
        final PipelineReplica[] pipelines = region.getReplicaPipelines( 0 );
        assertEquals( 3, pipelines.length );
        final PipelineReplica pipeline0 = pipelines[ 0 ];
        final PipelineReplica pipeline1 = pipelines[ 1 ];
        final PipelineReplica pipeline2 = pipelines[ 2 ];
        assertDefaultOperatorQueue( pipeline0, ex.operatorDef1.getInputPortCount() );
        assertEmptyPipelineQueue( pipeline0 );
        assertDefaultOperatorQueue( pipeline1, ex.operatorDef2.getInputPortCount() );
        assertDefaultPipelineQueue( pipeline1 );
        assertDefaultOperatorQueue( pipeline2, ex.operatorDef3.getInputPortCount() );
        assertEmptyPipelineQueue( pipeline2 );

        assertEquals( new PipelineReplicaId( regionDef.getRegionId(), 0, 0 ), pipeline0.id() );
        assertEquals( new PipelineReplicaId( regionDef.getRegionId(), 1, 0 ), pipeline1.id() );
        assertEquals( new PipelineReplicaId( regionDef.getRegionId(), 2, 0 ), pipeline2.id() );
        assertEquals( 1, pipeline0.getOperatorCount() );
        assertEquals( 1, pipeline1.getOperatorCount() );
        assertEquals( 1, pipeline2.getOperatorCount() );

        final OperatorReplica operatorReplica1 = pipeline0.getOperatorReplica( 0 );

        assertOperatorDef( operatorReplica1, 0, ex.operatorDef1 );
        assertDefaultOperatorQueue( operatorReplica1, ex.operatorDef1.getInputPortCount(), MULTI_THREADED );
        assertNull( kvStoreManager.getDefaultKVStore( region.getRegionId(), ex.operatorDef1.getId() ) );
        assertBlockingTupleQueueDrainerPool( operatorReplica1 );

        final OperatorReplica operatorReplica2 = pipeline1.getOperatorReplica( 0 );
        assertOperatorDef( operatorReplica2, 0, ex.operatorDef2 );
        assertPartitionedOperatorQueue( operatorReplica2 );
        assertNotNull( kvStoreManager.getPartitionedKVStores( region.getRegionId(), ex.operatorDef2.getId() ) );
        assertNonBlockingTupleQueueDrainerPool( operatorReplica2 );

        final OperatorReplica operatorReplica3 = pipeline2.getOperatorReplica( 0 );
        assertOperatorDef( operatorReplica3, 0, ex.operatorDef3 );
        assertDefaultOperatorQueue( operatorReplica3, ex.operatorDef3.getInputPortCount(), MULTI_THREADED );
        assertNull( kvStoreManager.getDefaultKVStore( region.getRegionId(), ex.operatorDef3.getId() ) );
        assertBlockingTupleQueueDrainerPool( operatorReplica3 );

        assertPipelineReplicaMeter( pipeline0 );
        assertPipelineReplicaMeter( pipeline1 );
        assertPipelineReplicaMeter( pipeline2 );
        assertNotEquals( pipeline0.getMeter(), pipeline1.getMeter() );
        assertNotEquals( pipeline0.getMeter(), pipeline2.getMeter() );
        assertNotEquals( pipeline1.getMeter(), pipeline2.getMeter() );
    }

    @Test
    public void test_operatorFusionStartIndices_with_singleScheduleWhenAtLeastOneTupleAvailable ()
    {
        final SchedulingStrategy[] schedulingStrategies = { scheduleWhenTuplesAvailableOnDefaultPort( 1 ) };

        assertArrayEquals( new int[] { 0 }, findFusionStartIndices( schedulingStrategies ) );
    }

    @Test
    public void test_operatorFusionStartIndices_with_multipleScheduleWhenAtLeastOneTupleAvailable ()
    {
        final SchedulingStrategy[] schedulingStrategies = { scheduleWhenTuplesAvailableOnDefaultPort( 1 ),
                                                            scheduleWhenTuplesAvailableOnDefaultPort( 1 ),
                                                            scheduleWhenTuplesAvailableOnDefaultPort( 1 ) };

        assertArrayEquals( new int[] { 0 }, findFusionStartIndices( schedulingStrategies ) );
    }

    @Test
    public void test_operatorFusionStartIndices_with_scheduleWhenAvailableFollowedByMultipleScheduleWhenAtLeastOneTupleAvailable ()
    {
        final SchedulingStrategy[] schedulingStrategies = { ScheduleWhenAvailable.INSTANCE,
                                                            scheduleWhenTuplesAvailableOnDefaultPort( 1 ),
                                                            scheduleWhenTuplesAvailableOnDefaultPort( 1 ),
                                                            scheduleWhenTuplesAvailableOnDefaultPort( 1 ) };

        assertArrayEquals( new int[] { 0 }, findFusionStartIndices( schedulingStrategies ) );
    }

    @Test
    public void test_operatorFusionStartIndices_with_scheduleWhenAtLeastOneTupleAvailableWithMultiplePorts ()
    {
        final SchedulingStrategy[] schedulingStrategies = { scheduleWhenTuplesAvailableOnDefaultPort( 1 ),
                                                            scheduleWhenTuplesAvailableOnAny( AT_LEAST, 2, 1, 0, 1 ),
                                                            scheduleWhenTuplesAvailableOnDefaultPort( 1 ) };

        assertArrayEquals( new int[] { 0 }, findFusionStartIndices( schedulingStrategies ) );
    }

    @Test
    public void test_operatorFusionStartIndices_with_scheduleWhenAtLeastMultipleTuplesAvailable ()
    {
        final SchedulingStrategy[] schedulingStrategies = { scheduleWhenTuplesAvailableOnDefaultPort( 1 ),
                                                            scheduleWhenTuplesAvailableOnDefaultPort( 1 ),
                                                            scheduleWhenTuplesAvailableOnDefaultPort( 1 ),
                                                            scheduleWhenTuplesAvailableOnDefaultPort( 2 ),
                                                            scheduleWhenTuplesAvailableOnDefaultPort( 1 ) };

        assertArrayEquals( new int[] { 0, 3 }, findFusionStartIndices( schedulingStrategies ) );
    }

    @Test
    public void test_operatorFusionStartIndices_with_scheduleWhenExactlySingleTupleAvailable ()
    {
        final SchedulingStrategy[] schedulingStrategies = { scheduleWhenTuplesAvailableOnDefaultPort( 1 ),
                                                            scheduleWhenTuplesAvailableOnDefaultPort( 1 ),
                                                            scheduleWhenTuplesAvailableOnDefaultPort( 1 ),
                                                            scheduleWhenTuplesAvailableOnDefaultPort( EXACT, 1 ),
                                                            scheduleWhenTuplesAvailableOnDefaultPort( 1 ) };

        assertArrayEquals( new int[] { 0, 3 }, findFusionStartIndices( schedulingStrategies ) );
    }

    @Test
    public void test_operatorFusionStartIndices_with_scheduleWhenExactlyMultipleTuplesAvailable ()
    {
        final SchedulingStrategy[] schedulingStrategies = { scheduleWhenTuplesAvailableOnDefaultPort( 1 ),
                                                            scheduleWhenTuplesAvailableOnDefaultPort( 1 ),
                                                            scheduleWhenTuplesAvailableOnDefaultPort( 1 ),
                                                            scheduleWhenTuplesAvailableOnDefaultPort( EXACT, 5 ),
                                                            scheduleWhenTuplesAvailableOnDefaultPort( 1 ) };

        assertArrayEquals( new int[] { 0, 3 }, findFusionStartIndices( schedulingStrategies ) );
    }

    @Test
    public void test_operatorFusionStartIndices_with_scheduleWhenAtLeastOneButSameNumberOfTuplesAvailableWithMultiplePorts ()
    {
        final SchedulingStrategy[] schedulingStrategies = { scheduleWhenTuplesAvailableOnDefaultPort( 1 ),
                                                            scheduleWhenTuplesAvailableOnAll( AT_LEAST_BUT_SAME_ON_ALL_PORTS, 2, 1, 0, 1 ),
                                                            scheduleWhenTuplesAvailableOnDefaultPort( 1 ) };

        assertArrayEquals( new int[] { 0, 1 }, findFusionStartIndices( schedulingStrategies ) );
    }

    @Test
    public void test_operatorFusionStartIndices_with_scheduleWhenAtLeastMultipleButSameNumberOfTuplesAvailableWithMultiplePorts ()
    {
        final SchedulingStrategy[] schedulingStrategies = { scheduleWhenTuplesAvailableOnDefaultPort( 1 ),
                                                            scheduleWhenTuplesAvailableOnAll( AT_LEAST_BUT_SAME_ON_ALL_PORTS, 2, 2, 0, 1 ),
                                                            scheduleWhenTuplesAvailableOnDefaultPort( 1 ) };

        assertArrayEquals( new int[] { 0, 1 }, findFusionStartIndices( schedulingStrategies ) );
    }

    static void assertPipelineReplicaMeter ( final PipelineReplica pipelineReplica )
    {
        final PipelineReplicaMeter pipelineReplicaMeter = pipelineReplica.getMeter();
        for ( OperatorReplica operatorReplica : pipelineReplica.getOperators() )
        {
            assertTrue( pipelineReplicaMeter == operatorReplica.getMeter() );
        }
    }

    static void assertDefaultPipelineQueue ( final PipelineReplica pipeline )
    {
        assertTrue( pipeline.getQueue() instanceof DefaultOperatorQueue );
    }

    static void assertEmptyPipelineQueue ( final PipelineReplica pipeline )
    {
        assertTrue( pipeline.getQueue() instanceof EmptyOperatorQueue );
    }

    static void assertEmptyOperatorQueue ( final PipelineReplica pipeline, final int inputPortCount )
    {
        final OperatorQueue pipelineQueue = pipeline.getEffectiveQueue();
        assertTrue( pipelineQueue instanceof EmptyOperatorQueue );
        assertEquals( inputPortCount, pipelineQueue.getInputPortCount() );
    }

    static void assertDefaultOperatorQueue ( final PipelineReplica pipeline, final int inputPortCount )
    {
        final OperatorQueue pipelineQueue = pipeline.getEffectiveQueue();
        assertTrue( pipelineQueue instanceof DefaultOperatorQueue );
        assertEquals( MULTI_THREADED, ( (DefaultOperatorQueue) pipelineQueue ).getThreadingPref() );
        assertEquals( inputPortCount, pipelineQueue.getInputPortCount() );
    }

    static void assertOperatorDef ( final OperatorReplica operatorReplica, final int idx, final OperatorDef operatorDef )
    {
        assertTrue( operatorReplica.getOperatorDef( idx ) == operatorDef );
    }

    static void assertEmptyOperatorQueue ( final OperatorReplica operatorReplica )
    {
        final OperatorQueue operatorQueue = operatorReplica.getQueue();
        assertTrue( operatorQueue instanceof EmptyOperatorQueue );
        assertEquals( operatorReplica.getOperatorDef( 0 ).getId(), operatorQueue.getOperatorId() );
    }

    static void assertDefaultOperatorQueue ( final OperatorReplica operatorReplica,
                                             final int inputPortCount,
                                             final ThreadingPref threadingPref )
    {
        final OperatorQueue operatorQueue = operatorReplica.getQueue();
        assertTrue( operatorQueue instanceof DefaultOperatorQueue );
        assertEquals( threadingPref, ( (DefaultOperatorQueue) operatorQueue ).getThreadingPref() );
        assertEquals( inputPortCount, operatorQueue.getInputPortCount() );
    }

    static void assertPartitionedOperatorQueue ( final OperatorReplica operatorReplica )
    {
        final OperatorQueue operatorQueue = operatorReplica.getQueue();
        assertTrue( operatorQueue instanceof PartitionedOperatorQueue );
        assertEquals( operatorReplica.getOperatorDef( 0 ).getId(), operatorQueue.getOperatorId() );
    }

    static void assertBlockingTupleQueueDrainerPool ( final OperatorReplica operatorReplica )
    {
        assertTrue( operatorReplica.getDrainerPool() instanceof BlockingTupleQueueDrainerPool );
    }

    static void assertNonBlockingTupleQueueDrainerPool ( final OperatorReplica operatorReplica )
    {
        assertTrue( operatorReplica.getDrainerPool() instanceof NonBlockingTupleQueueDrainerPool );
    }

    @OperatorSpec( type = STATELESS, inputPortCount = 1, outputPortCount = 1 )
    public static class StatelessOperatorWithSingleInputOutputPort extends NopOperator
    {
        @Override
        public SchedulingStrategy init ( final InitCtx ctx )
        {
            return scheduleWhenTuplesAvailableOnDefaultPort( 1 );
        }
    }


    @OperatorSpec( type = STATELESS, inputPortCount = 0, outputPortCount = 1 )
    public static class StatelessOperatorWithZeroInputSingleOutputPort extends NopOperator
    {
        @Override
        public SchedulingStrategy init ( final InitCtx ctx )
        {
            return ScheduleWhenAvailable.INSTANCE;
        }
    }


    @OperatorSpec( type = PARTITIONED_STATEFUL, inputPortCount = 1, outputPortCount = 1 )
    public static class PartitionedStatefulOperatorWithSingleInputOutputPort extends NopOperator
    {
        @Override
        public SchedulingStrategy init ( final InitCtx ctx )
        {
            return scheduleWhenTuplesAvailableOnDefaultPort( 1 );
        }
    }


    @OperatorSpec( type = STATEFUL, inputPortCount = 1, outputPortCount = 1 )
    public static class StatefulOperatorWithSingleInputOutputPort extends NopOperator
    {
        @Override
        public SchedulingStrategy init ( final InitCtx ctx )
        {
            return scheduleWhenTuplesAvailableOnDefaultPort( 1 );
        }
    }


    @OperatorSpec( type = STATEFUL, inputPortCount = 1, outputPortCount = 1 )
    public static class NonFusibleStatefulOperatorWithSingleInputOutputPort extends NopOperator
    {
        @Override
        public SchedulingStrategy init ( final InitCtx ctx )
        {
            return scheduleWhenTuplesAvailableOnDefaultPort( 2 );
        }
    }


    @OperatorSpec( type = STATEFUL, inputPortCount = 0, outputPortCount = 1 )
    public static class StatefulOperatorWithZeroInputSingleOutputPort extends NopOperator
    {
        @Override
        public SchedulingStrategy init ( final InitCtx ctx )
        {
            return ScheduleWhenAvailable.INSTANCE;
        }
    }


    static class NopOperator implements Operator
    {

        @Override
        public SchedulingStrategy init ( final InitCtx ctx )
        {
            throw new UnsupportedOperationException();
        }


        @Override
        public final void invoke ( final InvocationCtx ctx )
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

        final OperatorRuntimeSchemaBuilder operatorRuntimeSchemaBuilder1 = new OperatorRuntimeSchemaBuilder( 1, 1 );

        final OperatorDef operatorDef0;

        final OperatorDef operatorDef1;

        final FlowDef flow;

        FlowExample4 ()
        {
            operatorRuntimeSchemaBuilder0.addOutputField( 0, "field2", Integer.class );
            operatorRuntimeSchemaBuilder1.addInputField( 0, "field2", Integer.class ).addOutputField( 0, "field3", Integer.class );
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

        final OperatorRuntimeSchemaBuilder operatorRuntimeSchemaBuilder2 = new OperatorRuntimeSchemaBuilder( 1, 1 );

        final OperatorDef operatorDef0;

        final OperatorDef operatorDef1;

        final OperatorDef operatorDef2;

        final FlowDef flow;

        FlowExample5 ()
        {
            operatorRuntimeSchemaBuilder0.addOutputField( 0, "field2", Integer.class );
            operatorRuntimeSchemaBuilder1.addInputField( 0, "field2", Integer.class ).addOutputField( 0, "field2", Integer.class );
            operatorRuntimeSchemaBuilder2.addInputField( 0, "field2", Integer.class ).addOutputField( 0, "field3", Integer.class );
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

        final OperatorRuntimeSchemaBuilder operatorRuntimeSchemaBuilder2 = new OperatorRuntimeSchemaBuilder( 1, 1 );

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


    public static class FlowExample7
    {

        final OperatorDef operatorDef0 = OperatorDefBuilder.newInstance( "op0", StatefulOperatorWithZeroInputSingleOutputPort.class )
                                                           .build();

        final OperatorDef operatorDef1 = OperatorDefBuilder.newInstance( "op1", StatefulOperatorWithSingleInputOutputPort.class ).build();

        final OperatorDef operatorDef2 = OperatorDefBuilder.newInstance( "op2", NonFusibleStatefulOperatorWithSingleInputOutputPort.class )
                                                           .build();

        final FlowDef flow = new FlowDefBuilder().add( operatorDef0 )
                                                 .add( operatorDef1 )
                                                 .add( operatorDef2 )
                                                 .connect( "op0", "op1" )
                                                 .connect( "op1", "op2" )
                                                 .build();

    }

}
