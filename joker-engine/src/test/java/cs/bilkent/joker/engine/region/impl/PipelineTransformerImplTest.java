package cs.bilkent.joker.engine.region.impl;

import org.junit.Test;

import cs.bilkent.joker.engine.config.JokerConfig;
import static cs.bilkent.joker.engine.config.ThreadingPref.MULTI_THREADED;
import static cs.bilkent.joker.engine.config.ThreadingPref.SINGLE_THREADED;
import cs.bilkent.joker.engine.flow.PipelineId;
import cs.bilkent.joker.engine.flow.RegionDef;
import cs.bilkent.joker.engine.flow.RegionExecPlan;
import cs.bilkent.joker.engine.kvstore.impl.OperatorKVStoreManagerImpl;
import cs.bilkent.joker.engine.metric.PipelineReplicaMeter;
import cs.bilkent.joker.engine.partition.PartitionKeyExtractorFactory;
import cs.bilkent.joker.engine.partition.PartitionService;
import cs.bilkent.joker.engine.partition.impl.PartitionKeyExtractorFactoryImpl;
import cs.bilkent.joker.engine.partition.impl.PartitionServiceImpl;
import cs.bilkent.joker.engine.pipeline.OperatorReplica;
import cs.bilkent.joker.engine.pipeline.PipelineReplica;
import cs.bilkent.joker.engine.pipeline.UpstreamContext;
import cs.bilkent.joker.engine.pipeline.impl.invocation.FusedInvocationContext;
import cs.bilkent.joker.engine.pipeline.impl.invocation.FusedPartitionedInvocationContext;
import cs.bilkent.joker.engine.region.PipelineTransformer;
import cs.bilkent.joker.engine.region.Region;
import static cs.bilkent.joker.engine.region.impl.RegionExecPlanUtil.checkPipelineStartIndicesToMerge;
import static cs.bilkent.joker.engine.region.impl.RegionExecPlanUtil.checkPipelineStartIndicesToSplit;
import static cs.bilkent.joker.engine.region.impl.RegionManagerImplTest.assertPipelineReplicaMeter;
import cs.bilkent.joker.engine.tuplequeue.impl.OperatorQueueManagerImpl;
import cs.bilkent.joker.engine.tuplequeue.impl.drainer.pool.BlockingTupleQueueDrainerPool;
import cs.bilkent.joker.engine.tuplequeue.impl.drainer.pool.NonBlockingTupleQueueDrainerPool;
import cs.bilkent.joker.engine.tuplequeue.impl.operator.DefaultOperatorQueue;
import cs.bilkent.joker.engine.tuplequeue.impl.operator.EmptyOperatorQueue;
import cs.bilkent.joker.engine.tuplequeue.impl.operator.PartitionedOperatorQueue;
import cs.bilkent.joker.flow.FlowDef;
import cs.bilkent.joker.flow.FlowDefBuilder;
import cs.bilkent.joker.operator.InitializationContext;
import cs.bilkent.joker.operator.InvocationContext;
import cs.bilkent.joker.operator.Operator;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.OperatorDefBuilder;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.impl.DefaultInvocationContext;
import cs.bilkent.joker.operator.scheduling.ScheduleWhenAvailable;
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
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class PipelineTransformerImplTest extends AbstractJokerTest
{

    private static final int REGION_ID = 1;

    private final JokerConfig config = new JokerConfig();

    private final PartitionService partitionService = new PartitionServiceImpl( config );

    private final OperatorKVStoreManagerImpl operatorKVStoreManager = new OperatorKVStoreManagerImpl();

    private final PartitionKeyExtractorFactory partitionKeyExtractorFactory = new PartitionKeyExtractorFactoryImpl();

    private final OperatorQueueManagerImpl operatorQueueManager = new OperatorQueueManagerImpl( config, partitionKeyExtractorFactory );


    private final PipelineTransformer pipelineTransformer = new PipelineTransformerImpl( config,
                                                                                         partitionService,
                                                                                         operatorQueueManager,
                                                                                         operatorKVStoreManager,
                                                                                         partitionKeyExtractorFactory );

    private final RegionManagerImpl regionManager = new RegionManagerImpl( config,
                                                                           partitionService,
                                                                           operatorKVStoreManager, operatorQueueManager,
                                                                           pipelineTransformer,
                                                                           partitionKeyExtractorFactory );

    @Test
    public void shouldMergeAllPipelinesOfStatefulRegionWithFusibleOperator ()
    {
        final OperatorDef operatorDef0 = OperatorDefBuilder.newInstance( "op0", StatelessInput0Output1Operator.class ).build();
        final OperatorDef operatorDef1 = OperatorDefBuilder.newInstance( "op1", FusibleStatefulInput1Output1Operator.class ).build();
        final OperatorDef operatorDef2 = OperatorDefBuilder.newInstance( "op2", StatelessInput1Output1Operator.class ).build();
        final OperatorDef operatorDef3 = OperatorDefBuilder.newInstance( "op3", FusibleStatefulInput1Output1Operator.class ).build();
        final OperatorDef operatorDef4 = OperatorDefBuilder.newInstance( "op4", StatelessInput1Output1Operator.class ).build();
        final OperatorDef operatorDef5 = OperatorDefBuilder.newInstance( "op5", FusibleStatefulInput1Output1Operator.class ).build();

        final FlowDef flow = new FlowDefBuilder().add( operatorDef0 )
                                                 .add( operatorDef1 )
                                                 .add( operatorDef2 )
                                                 .add( operatorDef3 )
                                                 .add( operatorDef4 )
                                                 .add( operatorDef5 )
                                                 .connect( "op0", "op1" )
                                                 .connect( "op1", "op2" )
                                                 .connect( "op2", "op3" )
                                                 .connect( "op3", "op4" )
                                                 .connect( "op4", "op5" )
                                                 .build();

        final RegionDef regionDef = new RegionDef( REGION_ID,
                                                   STATEFUL,
                                                   emptyList(),
                                                   asList( operatorDef1, operatorDef2, operatorDef3, operatorDef4, operatorDef5 ) );

        final RegionExecPlan regionExecPlan = new RegionExecPlan( regionDef, asList( 0, 2, 4 ), 1 );
        final Region region = regionManager.createRegion( flow, regionExecPlan );
        initialize( region );

        operatorKVStoreManager.getDefaultKVStore( region.getRegionId(), operatorDef1.getId() ).getKVStore( null ).set( "key1", "val1" );

        operatorKVStoreManager.getDefaultKVStore( region.getRegionId(), operatorDef3.getId() ).getKVStore( null ).set( "key3", "val3" );

        operatorKVStoreManager.getDefaultKVStore( region.getRegionId(), operatorDef5.getId() ).getKVStore( null ).set( "key5", "val5" );

        final Region newRegion = pipelineTransformer.mergePipelines( region, asList( 0, 2, 4 ) );

        assertThat( newRegion.getExecPlan().getReplicaCount(), equalTo( 1 ) );
        assertThat( newRegion.getExecPlan().getPipelineStartIndices(), equalTo( singletonList( 0 ) ) );

        final PipelineReplica[] newPipelineReplicas = newRegion.getReplicaPipelines( 0 );
        assertThat( newPipelineReplicas.length, equalTo( 1 ) );

        final PipelineReplica newPipelineReplica = newPipelineReplicas[ 0 ];
        assertThat( newPipelineReplica.getOperatorReplicaCount(), equalTo( 1 ) );

        final OperatorReplica pipelineOperator0 = newPipelineReplica.getOperatorReplica( 0 );
        assertThat( pipelineOperator0.getOperatorCount(), equalTo( 5 ) );

        assertThat( pipelineOperator0.getOperatorDef( 0 ), equalTo( operatorDef1 ) );
        assertThat( pipelineOperator0.getOperatorDef( 1 ), equalTo( operatorDef2 ) );
        assertThat( pipelineOperator0.getOperatorDef( 2 ), equalTo( operatorDef3 ) );
        assertThat( pipelineOperator0.getOperatorDef( 3 ), equalTo( operatorDef4 ) );
        assertThat( pipelineOperator0.getOperatorDef( 4 ), equalTo( operatorDef5 ) );

        assertTrue( pipelineOperator0.getInvocationContext( 0 ) instanceof DefaultInvocationContext );
        assertTrue( pipelineOperator0.getInvocationContext( 1 ) instanceof FusedInvocationContext );
        assertTrue( pipelineOperator0.getInvocationContext( 2 ) instanceof FusedInvocationContext );
        assertTrue( pipelineOperator0.getInvocationContext( 3 ) instanceof FusedInvocationContext );
        assertTrue( pipelineOperator0.getInvocationContext( 4 ) instanceof FusedInvocationContext );

        assertThat( ( (DefaultOperatorQueue) pipelineOperator0.getQueue() ).getThreadingPref(), equalTo( MULTI_THREADED ) );
        assertTrue( pipelineOperator0.getQueue() == operatorQueueManager.getDefaultQueue( region.getRegionId(), operatorDef1.getId(), 0 ) );
        assertTrue( pipelineOperator0.getDrainerPool() instanceof BlockingTupleQueueDrainerPool );
        assertTrue( newPipelineReplica.getQueue() instanceof EmptyOperatorQueue );

        assertThat( operatorKVStoreManager.getDefaultKVStore( region.getRegionId(), operatorDef1.getId() ).getKVStore( null ).get( "key1" ),
                    equalTo( "val1" ) );

        assertThat( operatorKVStoreManager.getDefaultKVStore( region.getRegionId(), operatorDef3.getId() ).getKVStore( null ).get( "key3" ),
                    equalTo( "val3" ) );

        assertThat( operatorKVStoreManager.getDefaultKVStore( region.getRegionId(), operatorDef5.getId() ).getKVStore( null ).get( "key5" ),
                    equalTo( "val5" ) );

        final UpstreamContext[] upstreamContexts = region.getUpstreamContexts();
        assertTrue( pipelineOperator0.getUpstreamContext( 0 ) == upstreamContexts[ 0 ] );
        assertTrue( pipelineOperator0.getUpstreamContext( 1 ) == upstreamContexts[ 1 ] );
        assertTrue( pipelineOperator0.getUpstreamContext( 2 ) == upstreamContexts[ 2 ] );
        assertTrue( pipelineOperator0.getUpstreamContext( 3 ) == upstreamContexts[ 3 ] );
        assertTrue( pipelineOperator0.getUpstreamContext( 4 ) == upstreamContexts[ 4 ] );
        assertNull( pipelineOperator0.getDownstreamContext() );

        final PipelineReplicaMeter meter = newPipelineReplica.getMeter();
        assertThat( meter.getHeadOperatorId(), equalTo( pipelineOperator0.getOperatorDef( 0 ).getId() ) );
        assertThat( meter.getInputPortCount(), equalTo( pipelineOperator0.getOperatorDef( 0 ).getInputPortCount() ) );
        assertPipelineReplicaMeter( newPipelineReplica );
    }

    @Test
    public void shouldMergeAllPipelinesOfStatefulRegionWithNonFusibleOperator ()
    {
        final OperatorDef operatorDef0 = OperatorDefBuilder.newInstance( "op0", StatelessInput0Output1Operator.class ).build();
        final OperatorDef operatorDef1 = OperatorDefBuilder.newInstance( "op1", FusibleStatefulInput1Output1Operator.class ).build();
        final OperatorDef operatorDef2 = OperatorDefBuilder.newInstance( "op2", StatelessInput1Output1Operator.class ).build();
        final OperatorDef operatorDef3 = OperatorDefBuilder.newInstance( "op3", NonFusibleStatefulInput1Output1Operator.class ).build();
        final OperatorDef operatorDef4 = OperatorDefBuilder.newInstance( "op4", StatelessInput1Output1Operator.class ).build();
        final OperatorDef operatorDef5 = OperatorDefBuilder.newInstance( "op5", NonFusibleStatefulInput1Output1Operator.class ).build();

        final FlowDef flow = new FlowDefBuilder().add( operatorDef0 )
                                                 .add( operatorDef1 )
                                                 .add( operatorDef2 )
                                                 .add( operatorDef3 )
                                                 .add( operatorDef4 )
                                                 .add( operatorDef5 )
                                                 .connect( "op0", "op1" ).connect( "op1", "op2" ).connect( "op2", "op3" )
                                                 .connect( "op3", "op4" )
                                                 .connect( "op4", "op5" )
                                                 .build();

        final RegionDef regionDef = new RegionDef( REGION_ID,
                                                   STATEFUL,
                                                   emptyList(),
                                                   asList( operatorDef1, operatorDef2, operatorDef3, operatorDef4, operatorDef5 ) );

        final RegionExecPlan regionExecPlan = new RegionExecPlan( regionDef, asList( 0, 2, 4 ), 1 );
        final Region region = regionManager.createRegion( flow, regionExecPlan );
        initialize( region );

        operatorKVStoreManager.getDefaultKVStore( region.getRegionId(), operatorDef1.getId() ).getKVStore( null ).set( "key1", "val1" );

        operatorKVStoreManager.getDefaultKVStore( region.getRegionId(), operatorDef3.getId() ).getKVStore( null ).set( "key3", "val3" );

        operatorKVStoreManager.getDefaultKVStore( region.getRegionId(), operatorDef5.getId() ).getKVStore( null ).set( "key5", "val5" );

        final Region newRegion = pipelineTransformer.mergePipelines( region, asList( 0, 2, 4 ) );

        assertThat( newRegion.getExecPlan().getReplicaCount(), equalTo( 1 ) );
        assertThat( newRegion.getExecPlan().getPipelineStartIndices(), equalTo( singletonList( 0 ) ) );

        final PipelineReplica[] newPipelineReplicas = newRegion.getReplicaPipelines( 0 );
        assertThat( newPipelineReplicas.length, equalTo( 1 ) );

        final PipelineReplica newPipelineReplica = newPipelineReplicas[ 0 ];
        assertThat( newPipelineReplica.getOperatorReplicaCount(), equalTo( 3 ) );

        final OperatorReplica pipelineOperator0 = newPipelineReplica.getOperatorReplica( 0 );
        assertThat( pipelineOperator0.getOperatorCount(), equalTo( 2 ) );
        assertThat( pipelineOperator0.getOperatorDef( 0 ), equalTo( operatorDef1 ) );
        assertThat( pipelineOperator0.getOperatorDef( 1 ), equalTo( operatorDef2 ) );
        assertTrue( pipelineOperator0.getInvocationContext( 0 ) instanceof DefaultInvocationContext );
        assertTrue( pipelineOperator0.getInvocationContext( 1 ) instanceof FusedInvocationContext );
        assertThat( ( (DefaultOperatorQueue) pipelineOperator0.getQueue() ).getThreadingPref(), equalTo( MULTI_THREADED ) );
        assertTrue( pipelineOperator0.getQueue() == operatorQueueManager.getDefaultQueue( region.getRegionId(), operatorDef1.getId(), 0 ) );
        assertTrue( pipelineOperator0.getDrainerPool() instanceof BlockingTupleQueueDrainerPool );
        assertTrue( newPipelineReplica.getQueue() instanceof EmptyOperatorQueue );

        final OperatorReplica pipelineOperator1 = newPipelineReplica.getOperatorReplica( 1 );
        assertThat( pipelineOperator1.getOperatorCount(), equalTo( 2 ) );
        assertThat( pipelineOperator1.getOperatorDef( 0 ), equalTo( operatorDef3 ) );
        assertThat( pipelineOperator1.getOperatorDef( 1 ), equalTo( operatorDef4 ) );
        assertTrue( pipelineOperator1.getInvocationContext( 0 ) instanceof DefaultInvocationContext );
        assertTrue( pipelineOperator1.getInvocationContext( 1 ) instanceof FusedInvocationContext );
        assertThat( ( (DefaultOperatorQueue) pipelineOperator1.getQueue() ).getThreadingPref(), equalTo( SINGLE_THREADED ) );
        assertTrue( pipelineOperator1.getQueue() == operatorQueueManager.getDefaultQueue( region.getRegionId(), operatorDef3.getId(), 0 ) );
        assertTrue( pipelineOperator1.getDrainerPool() instanceof NonBlockingTupleQueueDrainerPool );

        final OperatorReplica pipelineOperator2 = newPipelineReplica.getOperatorReplica( 2 );
        assertThat( pipelineOperator2.getOperatorCount(), equalTo( 1 ) );
        assertThat( pipelineOperator2.getOperatorDef( 0 ), equalTo( operatorDef5 ) );
        assertTrue( pipelineOperator2.getInvocationContext( 0 ) instanceof DefaultInvocationContext );
        assertThat( ( (DefaultOperatorQueue) pipelineOperator2.getQueue() ).getThreadingPref(), equalTo( SINGLE_THREADED ) );
        assertTrue( pipelineOperator2.getQueue() == operatorQueueManager.getDefaultQueue( region.getRegionId(), operatorDef5.getId(), 0 ) );
        assertTrue( pipelineOperator2.getDrainerPool() instanceof NonBlockingTupleQueueDrainerPool );

        assertThat( operatorKVStoreManager.getDefaultKVStore( region.getRegionId(), operatorDef1.getId() ).getKVStore( null ).get( "key1" ),
                    equalTo( "val1" ) );

        assertThat( operatorKVStoreManager.getDefaultKVStore( region.getRegionId(), operatorDef3.getId() ).getKVStore( null ).get( "key3" ),
                    equalTo( "val3" ) );

        assertThat( operatorKVStoreManager.getDefaultKVStore( region.getRegionId(), operatorDef5.getId() ).getKVStore( null ).get( "key5" ),
                    equalTo( "val5" ) );

        final UpstreamContext[] upstreamContexts = region.getUpstreamContexts();
        assertTrue( pipelineOperator0.getUpstreamContext( 0 ) == upstreamContexts[ 0 ] );
        assertTrue( pipelineOperator0.getUpstreamContext( 1 ) == upstreamContexts[ 1 ] );
        assertTrue( pipelineOperator0.getDownstreamContext() == upstreamContexts[ 2 ] );
        assertTrue( pipelineOperator1.getUpstreamContext( 0 ) == upstreamContexts[ 2 ] );
        assertTrue( pipelineOperator1.getUpstreamContext( 1 ) == upstreamContexts[ 3 ] );
        assertTrue( pipelineOperator1.getDownstreamContext() == upstreamContexts[ 4 ] );
        assertTrue( pipelineOperator2.getUpstreamContext( 0 ) == upstreamContexts[ 4 ] );
        assertNull( pipelineOperator2.getDownstreamContext() );

        final PipelineReplicaMeter meter = newPipelineReplica.getMeter();
        assertThat( meter.getHeadOperatorId(), equalTo( pipelineOperator0.getOperatorDef( 0 ).getId() ) );
        assertThat( meter.getInputPortCount(), equalTo( pipelineOperator0.getOperatorDef( 0 ).getInputPortCount() ) );
        assertPipelineReplicaMeter( newPipelineReplica );
    }

    @Test
    public void shouldMergeAllPipelinesOfPartitionedStatefulRegion ()
    {
        final OperatorDef operatorDef0 = OperatorDefBuilder.newInstance( "op0", StatelessInput0Output1Operator.class ).build();
        final OperatorDef operatorDef1 = OperatorDefBuilder.newInstance( "op1", PartitionedStatefulInput1Output1Operator.class )
                                                           .setPartitionFieldNames( singletonList( "field" ) )
                                                           .build();
        final OperatorDef operatorDef2 = OperatorDefBuilder.newInstance( "op2", StatelessInput1Output1Operator.class ).build();
        final OperatorDef operatorDef3 = OperatorDefBuilder.newInstance( "op3", PartitionedStatefulInput1Output1Operator.class )
                                                           .setPartitionFieldNames( singletonList( "field" ) )
                                                           .build();
        final OperatorDef operatorDef4 = OperatorDefBuilder.newInstance( "op4", StatelessInput1Output1Operator.class ).build();
        final OperatorDef operatorDef5 = OperatorDefBuilder.newInstance( "op5", PartitionedStatefulInput1Output1Operator.class )
                                                           .setPartitionFieldNames( singletonList( "field" ) )
                                                           .build();

        final FlowDef flow = new FlowDefBuilder().add( operatorDef0 )
                                                 .add( operatorDef1 )
                                                 .add( operatorDef2 )
                                                 .add( operatorDef3 )
                                                 .add( operatorDef4 )
                                                 .add( operatorDef5 )
                                                 .connect( "op0", "op1" )
                                                 .connect( "op1", "op2" )
                                                 .connect( "op2", "op3" )
                                                 .connect( "op3", "op4" )
                                                 .connect( "op4", "op5" )
                                                 .build();

        final RegionDef regionDef = new RegionDef( REGION_ID, PARTITIONED_STATEFUL, singletonList( "field" ),
                                                   asList( operatorDef1, operatorDef2, operatorDef3, operatorDef4, operatorDef5 ) );

        final RegionExecPlan regionExecPlan = new RegionExecPlan( regionDef, asList( 0, 2 ), 1 );
        final Region region = regionManager.createRegion( flow, regionExecPlan );
        initialize( region );

        final Region newRegion = pipelineTransformer.mergePipelines( region, asList( 0, 2 ) );

        assertThat( newRegion.getExecPlan().getReplicaCount(), equalTo( 1 ) );
        assertThat( newRegion.getExecPlan().getPipelineStartIndices(), equalTo( singletonList( 0 ) ) );

        final PipelineReplica[] newPipelineReplicas = newRegion.getReplicaPipelines( 0 );
        assertThat( newPipelineReplicas.length, equalTo( 1 ) );

        final PipelineReplica newPipelineReplica = newPipelineReplicas[ 0 ];
        assertThat( newPipelineReplica.getOperatorReplicaCount(), equalTo( 1 ) );

        final OperatorReplica pipelineOperator0 = newPipelineReplica.getOperatorReplica( 0 );
        assertThat( pipelineOperator0.getOperatorCount(), equalTo( 5 ) );

        assertThat( pipelineOperator0.getOperatorDef( 0 ), equalTo( operatorDef1 ) );
        assertThat( pipelineOperator0.getOperatorDef( 1 ), equalTo( operatorDef2 ) );
        assertThat( pipelineOperator0.getOperatorDef( 2 ), equalTo( operatorDef3 ) );
        assertThat( pipelineOperator0.getOperatorDef( 3 ), equalTo( operatorDef4 ) );
        assertThat( pipelineOperator0.getOperatorDef( 4 ), equalTo( operatorDef5 ) );

        assertTrue( pipelineOperator0.getInvocationContext( 0 ) instanceof DefaultInvocationContext );
        assertTrue( pipelineOperator0.getInvocationContext( 1 ) instanceof FusedInvocationContext );
        assertTrue( pipelineOperator0.getInvocationContext( 2 ) instanceof FusedPartitionedInvocationContext );
        assertTrue( pipelineOperator0.getInvocationContext( 3 ) instanceof FusedInvocationContext );
        assertTrue( pipelineOperator0.getInvocationContext( 4 ) instanceof FusedPartitionedInvocationContext );

        assertTrue( pipelineOperator0.getQueue() instanceof PartitionedOperatorQueue );
        assertTrue(
                pipelineOperator0.getQueue() == operatorQueueManager.getPartitionedQueue( region.getRegionId(), operatorDef1.getId(), 0 ) );
        assertTrue( pipelineOperator0.getDrainerPool() instanceof NonBlockingTupleQueueDrainerPool );
        assertTrue( newPipelineReplica.getQueue() instanceof DefaultOperatorQueue );

        final UpstreamContext[] upstreamContexts = region.getUpstreamContexts();
        assertTrue( pipelineOperator0.getUpstreamContext( 0 ) == upstreamContexts[ 0 ] );
        assertTrue( pipelineOperator0.getUpstreamContext( 1 ) == upstreamContexts[ 1 ] );
        assertTrue( pipelineOperator0.getUpstreamContext( 2 ) == upstreamContexts[ 2 ] );
        assertTrue( pipelineOperator0.getUpstreamContext( 3 ) == upstreamContexts[ 3 ] );
        assertTrue( pipelineOperator0.getUpstreamContext( 4 ) == upstreamContexts[ 4 ] );
        assertNull( pipelineOperator0.getDownstreamContext() );

        final PipelineReplicaMeter pipelineReplicaMeter = newPipelineReplica.getMeter();
        final OperatorDef[] mergedOperatorDefs = newRegion.getExecPlan().getOperatorDefsByPipelineStartIndex( 0 );

        assertThat( pipelineReplicaMeter.getHeadOperatorId(), equalTo( mergedOperatorDefs[ 0 ].getId() ) );
        assertThat( pipelineReplicaMeter.getInputPortCount(), equalTo( mergedOperatorDefs[ 0 ].getInputPortCount() ) );

        assertPipelineReplicaMeter( newPipelineReplica );
    }

    @Test
    public void shouldMergeSingleOperatorPipelines ()
    {
        final OperatorDef operatorDef0 = OperatorDefBuilder.newInstance( "op0", StatefulInput0Output1Operator.class ).build();
        final OperatorDef operatorDef1 = OperatorDefBuilder.newInstance( "op1", PartitionedStatefulInput1Output1Operator.class )
                                                           .setPartitionFieldNames( singletonList( "field" ) )
                                                           .build();
        final OperatorDef operatorDef2 = OperatorDefBuilder.newInstance( "op2", StatelessInput1Output1Operator.class ).build();
        final OperatorDef operatorDef3 = OperatorDefBuilder.newInstance( "op3", StatelessInput1Output1Operator.class ).build();
        final OperatorDef operatorDef4 = OperatorDefBuilder.newInstance( "op4", StatelessInput1Output1Operator.class ).build();
        final OperatorDef operatorDef5 = OperatorDefBuilder.newInstance( "op5", StatelessInput1Output1Operator.class ).build();
        final OperatorDef operatorDef6 = OperatorDefBuilder.newInstance( "op6", StatelessInput1Output1Operator.class ).build();

        final FlowDef flow = new FlowDefBuilder().add( operatorDef0 )
                                                 .add( operatorDef1 )
                                                 .add( operatorDef2 )
                                                 .add( operatorDef3 )
                                                 .add( operatorDef4 )
                                                 .add( operatorDef5 )
                                                 .add( operatorDef6 )
                                                 .connect( "op0", "op1" )
                                                 .connect( "op1", "op2" )
                                                 .connect( "op2", "op3" )
                                                 .connect( "op3", "op4" )
                                                 .connect( "op4", "op5" )
                                                 .connect( "op5", "op6" )
                                                 .build();

        final RegionDef regionDef = new RegionDef( REGION_ID,
                                                   STATELESS,
                                                   emptyList(),
                                                   asList( operatorDef2, operatorDef3, operatorDef4, operatorDef5, operatorDef6 ) );

        final RegionExecPlan regionExecPlan = new RegionExecPlan( regionDef, asList( 0, 1, 2, 3, 4 ), 1 );
        final Region region = regionManager.createRegion( flow, regionExecPlan );
        initialize( region );

        final Region newRegion = pipelineTransformer.mergePipelines( region, asList( 0, 1, 2, 3, 4 ) );

        final PipelineReplica[] newPipelineReplicas = newRegion.getReplicaPipelines( 0 );
        assertThat( newPipelineReplicas.length, equalTo( 1 ) );

        final PipelineReplica newPipelineReplica = newPipelineReplicas[ 0 ];
        assertThat( newPipelineReplica.getOperatorReplicaCount(), equalTo( 1 ) );

        final OperatorReplica pipelineOperator0 = newPipelineReplica.getOperatorReplica( 0 );
        assertThat( pipelineOperator0.getOperatorCount(), equalTo( 5 ) );

        assertThat( pipelineOperator0.getOperatorDef( 0 ), equalTo( operatorDef2 ) );
        assertThat( pipelineOperator0.getOperatorDef( 1 ), equalTo( operatorDef3 ) );
        assertThat( pipelineOperator0.getOperatorDef( 2 ), equalTo( operatorDef4 ) );
        assertThat( pipelineOperator0.getOperatorDef( 3 ), equalTo( operatorDef5 ) );
        assertThat( pipelineOperator0.getOperatorDef( 4 ), equalTo( operatorDef6 ) );

        assertTrue( pipelineOperator0.getInvocationContext( 0 ) instanceof DefaultInvocationContext );
        assertTrue( pipelineOperator0.getInvocationContext( 1 ) instanceof FusedInvocationContext );
        assertTrue( pipelineOperator0.getInvocationContext( 2 ) instanceof FusedInvocationContext );
        assertTrue( pipelineOperator0.getInvocationContext( 3 ) instanceof FusedInvocationContext );
        assertTrue( pipelineOperator0.getInvocationContext( 4 ) instanceof FusedInvocationContext );

        assertThat( ( (DefaultOperatorQueue) pipelineOperator0.getQueue() ).getThreadingPref(), equalTo( MULTI_THREADED ) );
        assertTrue( pipelineOperator0.getQueue() == operatorQueueManager.getDefaultQueue( region.getRegionId(), operatorDef2.getId(), 0 ) );
        assertTrue( pipelineOperator0.getDrainerPool() instanceof BlockingTupleQueueDrainerPool );
        assertTrue( newPipelineReplica.getQueue() instanceof EmptyOperatorQueue );

        final UpstreamContext[] upstreamContexts = region.getUpstreamContexts();
        assertTrue( pipelineOperator0.getUpstreamContext( 0 ) == upstreamContexts[ 0 ] );
        assertTrue( pipelineOperator0.getUpstreamContext( 1 ) == upstreamContexts[ 1 ] );
        assertTrue( pipelineOperator0.getUpstreamContext( 2 ) == upstreamContexts[ 2 ] );
        assertTrue( pipelineOperator0.getUpstreamContext( 3 ) == upstreamContexts[ 3 ] );
        assertTrue( pipelineOperator0.getUpstreamContext( 4 ) == upstreamContexts[ 4 ] );
        assertNull( pipelineOperator0.getDownstreamContext() );

        final PipelineReplicaMeter meter = newPipelineReplica.getMeter();
        assertThat( meter.getHeadOperatorId(), equalTo( pipelineOperator0.getOperatorDef( 0 ).getId() ) );
        assertThat( meter.getInputPortCount(), equalTo( pipelineOperator0.getOperatorDef( 0 ).getInputPortCount() ) );
        assertPipelineReplicaMeter( newPipelineReplica );
    }

    @Test
    public void shouldCopyNonMergedPipelinesAtHeadOfTheRegion ()
    {
        final OperatorDef operatorDef0 = OperatorDefBuilder.newInstance( "op0", StatefulInput0Output1Operator.class ).build();
        final OperatorDef operatorDef1 = OperatorDefBuilder.newInstance( "op1", PartitionedStatefulInput1Output1Operator.class )
                                                           .setPartitionFieldNames( singletonList( "field" ) )
                                                           .build();
        final OperatorDef operatorDef2 = OperatorDefBuilder.newInstance( "op2", StatelessInput1Output1Operator.class ).build();
        final OperatorDef operatorDef3 = OperatorDefBuilder.newInstance( "op3", StatelessInput1Output1Operator.class ).build();
        final OperatorDef operatorDef4 = OperatorDefBuilder.newInstance( "op4", StatelessInput1Output1Operator.class ).build();
        final OperatorDef operatorDef5 = OperatorDefBuilder.newInstance( "op5", StatelessInput1Output1Operator.class ).build();
        final OperatorDef operatorDef6 = OperatorDefBuilder.newInstance( "op6", StatelessInput1Output1Operator.class ).build();

        final FlowDef flow = new FlowDefBuilder().add( operatorDef0 )
                                                 .add( operatorDef1 )
                                                 .add( operatorDef2 )
                                                 .add( operatorDef3 )
                                                 .add( operatorDef4 )
                                                 .add( operatorDef5 )
                                                 .add( operatorDef6 )
                                                 .connect( "op0", "op1" )
                                                 .connect( "op1", "op2" )
                                                 .connect( "op2", "op3" )
                                                 .connect( "op3", "op4" )
                                                 .connect( "op4", "op5" )
                                                 .connect( "op5", "op6" )
                                                 .build();

        final RegionDef regionDef = new RegionDef( REGION_ID,
                                                   PARTITIONED_STATEFUL,
                                                   singletonList( "field" ),
                                                   asList( operatorDef1,
                                                           operatorDef2,
                                                           operatorDef3,
                                                           operatorDef4,
                                                           operatorDef5,
                                                           operatorDef6 ) );

        final RegionExecPlan regionExecPlan = new RegionExecPlan( regionDef, asList( 0, 2, 3, 4 ), 2 );
        final Region region = regionManager.createRegion( flow, regionExecPlan );
        initialize( region );

        final Region newRegion = pipelineTransformer.mergePipelines( region, asList( 3, 4 ) );

        assertThat( newRegion.getPipelineReplicas( 0 ), equalTo( region.getPipelineReplicas( 0 ) ) );
        assertThat( newRegion.getPipelineReplicas( 1 ), equalTo( region.getPipelineReplicas( 1 ) ) );
    }

    @Test
    public void shouldCopyNonMergedPipelinesAtTailOfTheRegion ()
    {
        final OperatorDef operatorDef0 = OperatorDefBuilder.newInstance( "op0", StatefulInput0Output1Operator.class ).build();
        final OperatorDef operatorDef1 = OperatorDefBuilder.newInstance( "op1", PartitionedStatefulInput1Output1Operator.class )
                                                           .setPartitionFieldNames( singletonList( "field" ) )
                                                           .build();
        final OperatorDef operatorDef2 = OperatorDefBuilder.newInstance( "op2", StatelessInput1Output1Operator.class ).build();
        final OperatorDef operatorDef3 = OperatorDefBuilder.newInstance( "op3", StatelessInput1Output1Operator.class ).build();
        final OperatorDef operatorDef4 = OperatorDefBuilder.newInstance( "op4", StatelessInput1Output1Operator.class ).build();
        final OperatorDef operatorDef5 = OperatorDefBuilder.newInstance( "op5", StatelessInput1Output1Operator.class ).build();
        final OperatorDef operatorDef6 = OperatorDefBuilder.newInstance( "op6", StatelessInput1Output1Operator.class ).build();

        final FlowDef flow = new FlowDefBuilder().add( operatorDef0 )
                                                 .add( operatorDef1 )
                                                 .add( operatorDef2 )
                                                 .add( operatorDef3 )
                                                 .add( operatorDef4 )
                                                 .add( operatorDef5 )
                                                 .add( operatorDef6 )
                                                 .connect( "op0", "op1" )
                                                 .connect( "op1", "op2" )
                                                 .connect( "op2", "op3" )
                                                 .connect( "op3", "op4" )
                                                 .connect( "op4", "op5" )
                                                 .connect( "op5", "op6" )
                                                 .build();

        final RegionDef regionDef = new RegionDef( REGION_ID,
                                                   PARTITIONED_STATEFUL,
                                                   singletonList( "field" ),
                                                   asList( operatorDef1,
                                                           operatorDef2,
                                                           operatorDef3,
                                                           operatorDef4,
                                                           operatorDef5,
                                                           operatorDef6 ) );

        final RegionExecPlan regionExecPlan = new RegionExecPlan( regionDef, asList( 0, 2, 3, 4 ), 2 );
        final Region region = regionManager.createRegion( flow, regionExecPlan );
        initialize( region );

        final Region newRegion = pipelineTransformer.mergePipelines( region, asList( 0, 2 ) );

        assertThat( newRegion.getPipelineReplicas( 1 ), equalTo( region.getPipelineReplicas( 2 ) ) );
        assertThat( newRegion.getPipelineReplicas( 2 ), equalTo( region.getPipelineReplicas( 3 ) ) );
    }

    @Test
    public void testCheckPipelineStartIndicesToMerge ()
    {
        final OperatorDef operatorDef1 = OperatorDefBuilder.newInstance( "op0", PartitionedStatefulInput1Output1Operator.class )
                                                           .setPartitionFieldNames( singletonList( "field" ) )
                                                           .build();
        final OperatorDef operatorDef2 = OperatorDefBuilder.newInstance( "op2", StatelessInput1Output1Operator.class ).build();
        final OperatorDef operatorDef3 = OperatorDefBuilder.newInstance( "op3", StatelessInput1Output1Operator.class ).build();
        final OperatorDef operatorDef4 = OperatorDefBuilder.newInstance( "op4", StatelessInput1Output1Operator.class ).build();
        final OperatorDef operatorDef5 = OperatorDefBuilder.newInstance( "op5", StatelessInput1Output1Operator.class ).build();

        final RegionDef regionDef = new RegionDef( REGION_ID, PARTITIONED_STATEFUL, singletonList( "field" ),
                                                   asList( operatorDef1, operatorDef2, operatorDef3, operatorDef4, operatorDef5 ) );

        final RegionExecPlan regionExecPlan = new RegionExecPlan( regionDef, asList( 0, 2, 3, 4 ), 2 );

        assertFalse( checkPipelineStartIndicesToMerge( regionExecPlan, singletonList( 0 ) ) );
        assertFalse( checkPipelineStartIndicesToMerge( regionExecPlan, singletonList( -1 ) ) );
        assertFalse( checkPipelineStartIndicesToMerge( regionExecPlan, asList( 0, 2, 4 ) ) );

        assertTrue( checkPipelineStartIndicesToMerge( regionExecPlan, asList( 0, 2 ) ) );
        assertTrue( checkPipelineStartIndicesToMerge( regionExecPlan, asList( 0, 2, 3 ) ) );
        assertTrue( checkPipelineStartIndicesToMerge( regionExecPlan, asList( 0, 2, 3, 4 ) ) );
        assertTrue( checkPipelineStartIndicesToMerge( regionExecPlan, asList( 3, 4 ) ) );
    }

    @Test
    public void shouldSplitFusedOperatorOfStatefulRegion ()
    {
        final OperatorDef operatorDef0 = OperatorDefBuilder.newInstance( "op0", StatelessInput0Output1Operator.class ).build();
        final OperatorDef operatorDef1 = OperatorDefBuilder.newInstance( "op1", FusibleStatefulInput1Output1Operator.class ).build();
        final OperatorDef operatorDef2 = OperatorDefBuilder.newInstance( "op2", StatelessInput1Output1Operator.class ).build();
        final OperatorDef operatorDef3 = OperatorDefBuilder.newInstance( "op3", FusibleStatefulInput1Output1Operator.class ).build();
        final OperatorDef operatorDef4 = OperatorDefBuilder.newInstance( "op4", StatelessInput1Output1Operator.class ).build();
        final OperatorDef operatorDef5 = OperatorDefBuilder.newInstance( "op5", FusibleStatefulInput1Output1Operator.class ).build();

        final FlowDef flow = new FlowDefBuilder().add( operatorDef0 )
                                                 .add( operatorDef1 )
                                                 .add( operatorDef2 )
                                                 .add( operatorDef3 )
                                                 .add( operatorDef4 )
                                                 .add( operatorDef5 )
                                                 .connect( "op0", "op1" )
                                                 .connect( "op1", "op2" )
                                                 .connect( "op2", "op3" )
                                                 .connect( "op3", "op4" )
                                                 .connect( "op4", "op5" )
                                                 .build();

        final RegionDef regionDef = new RegionDef( REGION_ID,
                                                   STATEFUL,
                                                   emptyList(),
                                                   asList( operatorDef1, operatorDef2, operatorDef3, operatorDef4, operatorDef5 ) );

        final RegionExecPlan regionExecPlan = new RegionExecPlan( regionDef, singletonList( 0 ), 1 );
        final Region region = regionManager.createRegion( flow, regionExecPlan );
        initialize( region );

        operatorKVStoreManager.getDefaultKVStore( region.getRegionId(), operatorDef1.getId() ).getKVStore( null ).set( "key1", "val1" );

        operatorKVStoreManager.getDefaultKVStore( region.getRegionId(), operatorDef3.getId() ).getKVStore( null ).set( "key3", "val3" );

        operatorKVStoreManager.getDefaultKVStore( region.getRegionId(), operatorDef5.getId() ).getKVStore( null ).set( "key5", "val5" );

        final Region newRegion = pipelineTransformer.splitPipeline( region, asList( 0, 1, 4 ) );

        assertThat( newRegion.getExecPlan().getReplicaCount(), equalTo( 1 ) );
        assertThat( newRegion.getExecPlan().getPipelineStartIndices(), equalTo( asList( 0, 1, 4 ) ) );

        final PipelineReplica[] newPipelineReplicas = newRegion.getReplicaPipelines( 0 );
        assertThat( newPipelineReplicas[ 0 ].getOperatorReplicaCount(), equalTo( 1 ) );
        assertThat( newPipelineReplicas[ 1 ].getOperatorReplicaCount(), equalTo( 1 ) );
        assertThat( newPipelineReplicas[ 2 ].getOperatorReplicaCount(), equalTo( 1 ) );

        final OperatorReplica pipeline0Operator = newPipelineReplicas[ 0 ].getOperatorReplica( 0 );
        final OperatorReplica pipeline1Operator = newPipelineReplicas[ 1 ].getOperatorReplica( 0 );
        final OperatorReplica pipeline2Operator = newPipelineReplicas[ 2 ].getOperatorReplica( 0 );

        assertThat( pipeline0Operator.getOperatorCount(), equalTo( 1 ) );
        assertThat( pipeline1Operator.getOperatorCount(), equalTo( 3 ) );
        assertThat( pipeline2Operator.getOperatorCount(), equalTo( 1 ) );

        assertThat( newPipelineReplicas[ 0 ].id().pipelineId, equalTo( new PipelineId( REGION_ID, 0 ) ) );
        assertThat( ( (DefaultOperatorQueue) pipeline0Operator.getQueue() ).getThreadingPref(), equalTo( MULTI_THREADED ) );
        assertTrue( pipeline0Operator.getQueue() == operatorQueueManager.getDefaultQueue( region.getRegionId(), operatorDef1.getId(), 0 ) );
        assertTrue( pipeline0Operator.getDrainerPool() instanceof BlockingTupleQueueDrainerPool );
        assertTrue( newPipelineReplicas[ 0 ].getQueue() instanceof EmptyOperatorQueue );
        assertThat( pipeline0Operator.getOperatorDef( 0 ), equalTo( operatorDef1 ) );
        assertTrue( pipeline0Operator.getInvocationContext( 0 ) instanceof DefaultInvocationContext );

        assertThat( newPipelineReplicas[ 1 ].id().pipelineId, equalTo( new PipelineId( REGION_ID, 1 ) ) );
        assertTrue( pipeline1Operator.getQueue() == operatorQueueManager.getDefaultQueue( region.getRegionId(), operatorDef2.getId(), 0 ) );
        assertNull( operatorQueueManager.getDefaultQueue( region.getRegionId(), operatorDef3.getId(), 0 ) );
        assertNull( operatorQueueManager.getDefaultQueue( region.getRegionId(), operatorDef4.getId(), 0 ) );
        assertThat( ( (DefaultOperatorQueue) pipeline1Operator.getQueue() ).getThreadingPref(), equalTo( MULTI_THREADED ) );
        assertTrue( pipeline1Operator.getDrainerPool() instanceof BlockingTupleQueueDrainerPool );
        assertTrue( newPipelineReplicas[ 1 ].getQueue() instanceof EmptyOperatorQueue );
        assertThat( pipeline1Operator.getOperatorDef( 0 ), equalTo( operatorDef2 ) );
        assertThat( pipeline1Operator.getOperatorDef( 1 ), equalTo( operatorDef3 ) );
        assertThat( pipeline1Operator.getOperatorDef( 2 ), equalTo( operatorDef4 ) );
        assertTrue( pipeline1Operator.getInvocationContext( 0 ) instanceof DefaultInvocationContext );
        assertTrue( pipeline1Operator.getInvocationContext( 1 ) instanceof FusedInvocationContext );
        assertTrue( pipeline1Operator.getInvocationContext( 2 ) instanceof FusedInvocationContext );

        assertThat( newPipelineReplicas[ 2 ].id().pipelineId, equalTo( new PipelineId( REGION_ID, 4 ) ) );
        assertThat( ( (DefaultOperatorQueue) pipeline2Operator.getQueue() ).getThreadingPref(), equalTo( MULTI_THREADED ) );
        assertTrue( pipeline2Operator.getDrainerPool() instanceof BlockingTupleQueueDrainerPool );
        assertThat( pipeline2Operator.getOperatorDef( 0 ), equalTo( operatorDef5 ) );
        assertTrue( pipeline2Operator.getInvocationContext( 0 ) instanceof DefaultInvocationContext );

        assertThat( operatorKVStoreManager.getDefaultKVStore( region.getRegionId(), operatorDef1.getId() ).getKVStore( null ).get( "key1" ),
                    equalTo( "val1" ) );

        assertThat( operatorKVStoreManager.getDefaultKVStore( region.getRegionId(), operatorDef3.getId() ).getKVStore( null ).get( "key3" ),
                    equalTo( "val3" ) );

        assertThat( operatorKVStoreManager.getDefaultKVStore( region.getRegionId(), operatorDef5.getId() ).getKVStore( null ).get( "key5" ),
                    equalTo( "val5" ) );

        assertNull( pipeline0Operator.getDownstreamContext() );
        assertNull( pipeline1Operator.getDownstreamContext() );
        assertNull( pipeline2Operator.getDownstreamContext() );

        assertThat( newPipelineReplicas[ 0 ].getMeter().getHeadOperatorId(), equalTo( operatorDef1.getId() ) );
        assertThat( newPipelineReplicas[ 1 ].getMeter().getHeadOperatorId(), equalTo( operatorDef2.getId() ) );
        assertThat( newPipelineReplicas[ 2 ].getMeter().getHeadOperatorId(), equalTo( operatorDef5.getId() ) );

        for ( PipelineReplica newPipelineReplica : newPipelineReplicas )
        {
            assertPipelineReplicaMeter( newPipelineReplica );
        }
    }

    @Test
    public void shouldSplitNonFusedOperatorOfStatefulRegion ()
    {
        final OperatorDef operatorDef0 = OperatorDefBuilder.newInstance( "op0", StatelessInput0Output1Operator.class ).build();
        final OperatorDef operatorDef1 = OperatorDefBuilder.newInstance( "op1", FusibleStatefulInput1Output1Operator.class ).build();
        final OperatorDef operatorDef2 = OperatorDefBuilder.newInstance( "op2", StatelessInput1Output1Operator.class ).build();
        final OperatorDef operatorDef3 = OperatorDefBuilder.newInstance( "op3", NonFusibleStatefulInput1Output1Operator.class ).build();
        final OperatorDef operatorDef4 = OperatorDefBuilder.newInstance( "op4", NonFusibleStatefulInput1Output1Operator.class ).build();
        final OperatorDef operatorDef5 = OperatorDefBuilder.newInstance( "op5", NonFusibleStatefulInput1Output1Operator.class ).build();

        final FlowDef flow = new FlowDefBuilder().add( operatorDef0 )
                                                 .add( operatorDef1 )
                                                 .add( operatorDef2 )
                                                 .add( operatorDef3 )
                                                 .add( operatorDef4 )
                                                 .add( operatorDef5 )
                                                 .connect( "op0", "op1" )
                                                 .connect( "op1", "op2" )
                                                 .connect( "op2", "op3" )
                                                 .connect( "op3", "op4" )
                                                 .connect( "op4", "op5" )
                                                 .build();

        final RegionDef regionDef = new RegionDef( REGION_ID,
                                                   STATEFUL,
                                                   emptyList(),
                                                   asList( operatorDef1, operatorDef2, operatorDef3, operatorDef4, operatorDef5 ) );

        final RegionExecPlan regionExecPlan = new RegionExecPlan( regionDef, singletonList( 0 ), 1 );
        final Region region = regionManager.createRegion( flow, regionExecPlan );
        initialize( region );

        final Region newRegion = pipelineTransformer.splitPipeline( region, asList( 0, 2, 4 ) );

        assertThat( newRegion.getExecPlan().getReplicaCount(), equalTo( 1 ) );
        assertThat( newRegion.getExecPlan().getPipelineStartIndices(), equalTo( asList( 0, 2, 4 ) ) );

        final PipelineReplica[] newPipelineReplicas = newRegion.getReplicaPipelines( 0 );
        assertThat( newPipelineReplicas[ 0 ].getOperatorReplicaCount(), equalTo( 1 ) );
        assertThat( newPipelineReplicas[ 1 ].getOperatorReplicaCount(), equalTo( 2 ) );
        assertThat( newPipelineReplicas[ 2 ].getOperatorReplicaCount(), equalTo( 1 ) );

        final OperatorReplica pipeline0Operator = newPipelineReplicas[ 0 ].getOperatorReplica( 0 );
        final OperatorReplica pipeline1Operator0 = newPipelineReplicas[ 1 ].getOperatorReplica( 0 );
        final OperatorReplica pipeline1Operator1 = newPipelineReplicas[ 1 ].getOperatorReplica( 1 );
        final OperatorReplica pipeline2Operator = newPipelineReplicas[ 2 ].getOperatorReplica( 0 );

        assertThat( pipeline0Operator.getOperatorCount(), equalTo( 2 ) );
        assertThat( pipeline1Operator0.getOperatorCount(), equalTo( 1 ) );
        assertThat( pipeline1Operator1.getOperatorCount(), equalTo( 1 ) );
        assertThat( pipeline2Operator.getOperatorCount(), equalTo( 1 ) );

        assertThat( newPipelineReplicas[ 0 ].id().pipelineId, equalTo( new PipelineId( REGION_ID, 0 ) ) );
        assertThat( ( (DefaultOperatorQueue) pipeline0Operator.getQueue() ).getThreadingPref(), equalTo( MULTI_THREADED ) );
        assertTrue( pipeline0Operator.getQueue() == operatorQueueManager.getDefaultQueue( region.getRegionId(), operatorDef1.getId(), 0 ) );
        assertNull( operatorQueueManager.getDefaultQueue( region.getRegionId(), operatorDef2.getId(), 0 ) );
        assertTrue( pipeline0Operator.getDrainerPool() instanceof BlockingTupleQueueDrainerPool );
        assertTrue( newPipelineReplicas[ 0 ].getQueue() instanceof EmptyOperatorQueue );
        assertThat( pipeline0Operator.getOperatorDef( 0 ), equalTo( operatorDef1 ) );
        assertThat( pipeline0Operator.getOperatorDef( 1 ), equalTo( operatorDef2 ) );
        assertTrue( pipeline0Operator.getInvocationContext( 0 ) instanceof DefaultInvocationContext );
        assertTrue( pipeline0Operator.getInvocationContext( 1 ) instanceof FusedInvocationContext );

        assertThat( newPipelineReplicas[ 1 ].id().pipelineId, equalTo( new PipelineId( REGION_ID, 2 ) ) );
        assertTrue(
                pipeline1Operator0.getQueue() == operatorQueueManager.getDefaultQueue( region.getRegionId(), operatorDef3.getId(), 0 ) );
        assertThat( ( (DefaultOperatorQueue) pipeline1Operator0.getQueue() ).getThreadingPref(), equalTo( MULTI_THREADED ) );
        assertTrue( pipeline1Operator0.getDrainerPool() instanceof BlockingTupleQueueDrainerPool );
        assertTrue(
                pipeline1Operator1.getQueue() == operatorQueueManager.getDefaultQueue( region.getRegionId(), operatorDef4.getId(), 0 ) );
        assertThat( ( (DefaultOperatorQueue) pipeline1Operator1.getQueue() ).getThreadingPref(), equalTo( SINGLE_THREADED ) );
        assertTrue( pipeline1Operator1.getDrainerPool() instanceof NonBlockingTupleQueueDrainerPool );
        assertTrue( newPipelineReplicas[ 1 ].getQueue() instanceof EmptyOperatorQueue );
        assertThat( pipeline1Operator0.getOperatorDef( 0 ), equalTo( operatorDef3 ) );
        assertThat( pipeline1Operator1.getOperatorDef( 0 ), equalTo( operatorDef4 ) );
        assertTrue( pipeline1Operator0.getInvocationContext( 0 ) instanceof DefaultInvocationContext );
        assertTrue( pipeline1Operator1.getInvocationContext( 0 ) instanceof DefaultInvocationContext );

        assertThat( newPipelineReplicas[ 2 ].id().pipelineId, equalTo( new PipelineId( REGION_ID, 4 ) ) );
        assertThat( ( (DefaultOperatorQueue) pipeline2Operator.getQueue() ).getThreadingPref(), equalTo( MULTI_THREADED ) );
        assertTrue( pipeline2Operator.getDrainerPool() instanceof BlockingTupleQueueDrainerPool );
        assertThat( pipeline2Operator.getOperatorDef( 0 ), equalTo( operatorDef5 ) );
        assertTrue( pipeline2Operator.getInvocationContext( 0 ) instanceof DefaultInvocationContext );

        assertNull( pipeline0Operator.getDownstreamContext() );
        assertThat( pipeline1Operator0.getDownstreamContext(), equalTo( pipeline1Operator1.getUpstreamContext( 0 ) ) );
        assertNull( pipeline1Operator1.getDownstreamContext() );
        assertNull( pipeline2Operator.getDownstreamContext() );

        assertThat( newPipelineReplicas[ 0 ].getMeter().getHeadOperatorId(), equalTo( operatorDef1.getId() ) );
        assertThat( newPipelineReplicas[ 1 ].getMeter().getHeadOperatorId(), equalTo( operatorDef3.getId() ) );
        assertThat( newPipelineReplicas[ 2 ].getMeter().getHeadOperatorId(), equalTo( operatorDef5.getId() ) );

        for ( PipelineReplica newPipelineReplica : newPipelineReplicas )
        {
            assertPipelineReplicaMeter( newPipelineReplica );
        }
    }

    @Test
    public void shouldSplitSinglePipelineOfPartitionedStatefulRegion ()
    {
        final OperatorDef operatorDef0 = OperatorDefBuilder.newInstance( "op0", StatelessInput0Output1Operator.class ).build();
        final OperatorDef operatorDef1 = OperatorDefBuilder.newInstance( "op1", PartitionedStatefulInput1Output1Operator.class )
                                                           .setPartitionFieldNames( singletonList( "field" ) )
                                                           .build();
        final OperatorDef operatorDef2 = OperatorDefBuilder.newInstance( "op2", StatelessInput1Output1Operator.class ).build();
        final OperatorDef operatorDef3 = OperatorDefBuilder.newInstance( "op3", PartitionedStatefulInput1Output1Operator.class )
                                                           .setPartitionFieldNames( singletonList( "field" ) )
                                                           .build();
        final OperatorDef operatorDef4 = OperatorDefBuilder.newInstance( "op4", StatelessInput1Output1Operator.class ).build();
        final OperatorDef operatorDef5 = OperatorDefBuilder.newInstance( "op5", PartitionedStatefulInput1Output1Operator.class )
                                                           .setPartitionFieldNames( singletonList( "field" ) )
                                                           .build();

        final FlowDef flow = new FlowDefBuilder().add( operatorDef0 )
                                                 .add( operatorDef1 )
                                                 .add( operatorDef2 )
                                                 .add( operatorDef3 )
                                                 .add( operatorDef4 )
                                                 .add( operatorDef5 )
                                                 .connect( "op0", "op1" )
                                                 .connect( "op1", "op2" )
                                                 .connect( "op2", "op3" )
                                                 .connect( "op3", "op4" )
                                                 .connect( "op4", "op5" )
                                                 .build();

        final RegionDef regionDef = new RegionDef( REGION_ID, PARTITIONED_STATEFUL, singletonList( "field" ),
                                                   asList( operatorDef1, operatorDef2, operatorDef3, operatorDef4, operatorDef5 ) );

        final RegionExecPlan regionExecPlan = new RegionExecPlan( regionDef, singletonList( 0 ), 1 );
        final Region region = regionManager.createRegion( flow, regionExecPlan );
        initialize( region );

        final Region newRegion = pipelineTransformer.splitPipeline( region, asList( 0, 1, 4 ) );

        assertThat( newRegion.getExecPlan().getReplicaCount(), equalTo( 1 ) );
        assertThat( newRegion.getExecPlan().getPipelineStartIndices(), equalTo( asList( 0, 1, 4 ) ) );

        final PipelineReplica[] newPipelineReplicas = newRegion.getReplicaPipelines( 0 );
        assertThat( newPipelineReplicas[ 0 ].getOperatorReplicaCount(), equalTo( 1 ) );
        assertThat( newPipelineReplicas[ 0 ].getOperatorCount(), equalTo( 1 ) );
        assertThat( newPipelineReplicas[ 1 ].getOperatorReplicaCount(), equalTo( 1 ) );
        assertThat( newPipelineReplicas[ 1 ].getOperatorCount(), equalTo( 3 ) );
        assertThat( newPipelineReplicas[ 2 ].getOperatorReplicaCount(), equalTo( 1 ) );
        assertThat( newPipelineReplicas[ 2 ].getOperatorCount(), equalTo( 1 ) );

        final OperatorReplica pipelineOperator0 = newPipelineReplicas[ 0 ].getOperatorReplica( 0 );
        final OperatorReplica pipelineOperator1 = newPipelineReplicas[ 1 ].getOperatorReplica( 0 );
        final OperatorReplica pipelineOperator2 = newPipelineReplicas[ 2 ].getOperatorReplica( 0 );

        assertThat( newPipelineReplicas[ 0 ].id().pipelineId, equalTo( new PipelineId( REGION_ID, 0 ) ) );
        assertTrue( newPipelineReplicas[ 0 ].getQueue() instanceof DefaultOperatorQueue );
        assertThat( ( (DefaultOperatorQueue) newPipelineReplicas[ 0 ].getQueue() ).getThreadingPref(), equalTo( MULTI_THREADED ) );
        assertTrue( newPipelineReplicas[ 0 ].getQueue() == operatorQueueManager.getDefaultQueue( region.getRegionId(),
                                                                                                 operatorDef1.getId(),
                                                                                                 0 ) );

        assertTrue( pipelineOperator0.getQueue() instanceof PartitionedOperatorQueue );
        assertTrue(
                pipelineOperator0.getQueue() == operatorQueueManager.getPartitionedQueue( region.getRegionId(), operatorDef1.getId(), 0 ) );
        assertTrue( pipelineOperator0.getDrainerPool() instanceof NonBlockingTupleQueueDrainerPool );
        assertThat( pipelineOperator0.getOperatorDef( 0 ), equalTo( operatorDef1 ) );
        assertTrue( pipelineOperator0.getInvocationContext( 0 ) instanceof DefaultInvocationContext );

        assertThat( newPipelineReplicas[ 1 ].id().pipelineId, equalTo( new PipelineId( REGION_ID, 1 ) ) );
        assertTrue( newPipelineReplicas[ 1 ].getQueue() instanceof EmptyOperatorQueue );
        assertThat( ( (DefaultOperatorQueue) pipelineOperator1.getQueue() ).getThreadingPref(), equalTo( MULTI_THREADED ) );
        assertTrue( pipelineOperator1.getDrainerPool() instanceof BlockingTupleQueueDrainerPool );
        assertNull( operatorQueueManager.getDefaultQueue( region.getRegionId(), operatorDef3.getId(), 0 ) );
        assertNull( operatorQueueManager.getPartitionedQueues( region.getRegionId(), operatorDef3.getId() ) );
        assertNull( operatorQueueManager.getDefaultQueue( region.getRegionId(), operatorDef4.getId(), 0 ) );
        assertThat( pipelineOperator1.getOperatorDef( 0 ), equalTo( operatorDef2 ) );
        assertThat( pipelineOperator1.getOperatorDef( 1 ), equalTo( operatorDef3 ) );
        assertThat( pipelineOperator1.getOperatorDef( 2 ), equalTo( operatorDef4 ) );
        assertTrue( pipelineOperator1.getInvocationContext( 0 ) instanceof DefaultInvocationContext );
        assertTrue( pipelineOperator1.getInvocationContext( 1 ) instanceof FusedPartitionedInvocationContext );
        assertTrue( pipelineOperator1.getInvocationContext( 2 ) instanceof FusedInvocationContext );

        assertThat( newPipelineReplicas[ 2 ].id().pipelineId, equalTo( new PipelineId( REGION_ID, 4 ) ) );
        assertTrue( newPipelineReplicas[ 2 ].getQueue() instanceof DefaultOperatorQueue );
        assertThat( ( (DefaultOperatorQueue) newPipelineReplicas[ 2 ].getQueue() ).getThreadingPref(), equalTo( MULTI_THREADED ) );
        assertTrue( newPipelineReplicas[ 2 ].getQueue() == operatorQueueManager.getDefaultQueue( region.getRegionId(),
                                                                                                 operatorDef5.getId(),
                                                                                                 0 ) );
        assertTrue( pipelineOperator2.getQueue() instanceof PartitionedOperatorQueue );
        assertTrue( pipelineOperator2.getDrainerPool() instanceof NonBlockingTupleQueueDrainerPool );
        assertThat( pipelineOperator2.getOperatorDef( 0 ), equalTo( operatorDef5 ) );
        assertTrue( pipelineOperator2.getInvocationContext( 0 ) instanceof DefaultInvocationContext );

        assertNull( pipelineOperator0.getDownstreamContext() );
        assertNull( pipelineOperator1.getDownstreamContext() );
        assertNull( pipelineOperator2.getDownstreamContext() );

        final PipelineReplicaMeter meter0 = newPipelineReplicas[ 0 ].getMeter();
        assertThat( meter0.getHeadOperatorId(), equalTo( pipelineOperator0.getOperatorDef( 0 ).getId() ) );
        assertThat( meter0.getInputPortCount(), equalTo( pipelineOperator0.getOperatorDef( 0 ).getInputPortCount() ) );

        final PipelineReplicaMeter meter1 = newPipelineReplicas[ 1 ].getMeter();
        assertThat( meter1.getHeadOperatorId(), equalTo( pipelineOperator1.getOperatorDef( 0 ).getId() ) );
        assertThat( meter1.getInputPortCount(), equalTo( pipelineOperator1.getOperatorDef( 0 ).getInputPortCount() ) );

        final PipelineReplicaMeter meter2 = newPipelineReplicas[ 2 ].getMeter();
        assertThat( meter2.getHeadOperatorId(), equalTo( pipelineOperator2.getOperatorDef( 0 ).getId() ) );
        assertThat( meter2.getInputPortCount(), equalTo( pipelineOperator2.getOperatorDef( 0 ).getInputPortCount() ) );

        for ( PipelineReplica p : newPipelineReplicas )
        {
            assertPipelineReplicaMeter( p );
        }
    }

    @Test
    public void shouldSplitPipelineIntoSingleOperatorPipelines ()
    {
        final OperatorDef operatorDef0 = OperatorDefBuilder.newInstance( "op0", StatefulInput0Output1Operator.class ).build();
        final OperatorDef operatorDef1 = OperatorDefBuilder.newInstance( "op1", PartitionedStatefulInput1Output1Operator.class )
                                                           .setPartitionFieldNames( singletonList( "field" ) )
                                                           .build();
        final OperatorDef operatorDef2 = OperatorDefBuilder.newInstance( "op2", StatelessInput1Output1Operator.class ).build();
        final OperatorDef operatorDef3 = OperatorDefBuilder.newInstance( "op3", StatelessInput1Output1Operator.class ).build();
        final OperatorDef operatorDef4 = OperatorDefBuilder.newInstance( "op4", StatelessInput1Output1Operator.class ).build();
        final OperatorDef operatorDef5 = OperatorDefBuilder.newInstance( "op5", StatelessInput1Output1Operator.class ).build();
        final OperatorDef operatorDef6 = OperatorDefBuilder.newInstance( "op6", StatelessInput1Output1Operator.class ).build();

        final FlowDef flow = new FlowDefBuilder().add( operatorDef0 )
                                                 .add( operatorDef1 )
                                                 .add( operatorDef2 )
                                                 .add( operatorDef3 )
                                                 .add( operatorDef4 )
                                                 .add( operatorDef5 )
                                                 .add( operatorDef6 )
                                                 .connect( "op0", "op1" )
                                                 .connect( "op1", "op2" )
                                                 .connect( "op2", "op3" )
                                                 .connect( "op3", "op4" )
                                                 .connect( "op4", "op5" )
                                                 .connect( "op5", "op6" )
                                                 .build();

        final RegionDef regionDef = new RegionDef( REGION_ID,
                                                   STATELESS,
                                                   emptyList(),
                                                   asList( operatorDef2, operatorDef3, operatorDef4, operatorDef5, operatorDef6 ) );

        final RegionExecPlan regionExecPlan = new RegionExecPlan( regionDef, singletonList( 0 ), 1 );
        final Region region = regionManager.createRegion( flow, regionExecPlan );
        initialize( region );

        final Region newRegion = pipelineTransformer.splitPipeline( region, asList( 0, 1, 2, 3, 4 ) );

        final PipelineReplica[] newPipelineReplicas = newRegion.getReplicaPipelines( 0 );
        final OperatorReplica pipelineOperator0 = newPipelineReplicas[ 0 ].getOperatorReplica( 0 );
        final OperatorReplica pipelineOperator1 = newPipelineReplicas[ 1 ].getOperatorReplica( 0 );
        final OperatorReplica pipelineOperator2 = newPipelineReplicas[ 2 ].getOperatorReplica( 0 );
        final OperatorReplica pipelineOperator3 = newPipelineReplicas[ 3 ].getOperatorReplica( 0 );
        final OperatorReplica pipelineOperator4 = newPipelineReplicas[ 4 ].getOperatorReplica( 0 );

        assertThat( newPipelineReplicas[ 0 ].id().pipelineId, equalTo( new PipelineId( REGION_ID, 0 ) ) );
        assertTrue( newPipelineReplicas[ 0 ].getQueue() instanceof EmptyOperatorQueue );
        assertThat( ( (DefaultOperatorQueue) pipelineOperator0.getQueue() ).getThreadingPref(), equalTo( MULTI_THREADED ) );
        assertTrue( pipelineOperator0.getDrainerPool() instanceof BlockingTupleQueueDrainerPool );
        assertThat( newPipelineReplicas[ 1 ].id().pipelineId, equalTo( new PipelineId( REGION_ID, 1 ) ) );
        assertTrue( newPipelineReplicas[ 1 ].getQueue() instanceof EmptyOperatorQueue );
        assertThat( ( (DefaultOperatorQueue) pipelineOperator1.getQueue() ).getThreadingPref(), equalTo( MULTI_THREADED ) );
        assertTrue( pipelineOperator1.getDrainerPool() instanceof BlockingTupleQueueDrainerPool );
        assertThat( newPipelineReplicas[ 2 ].id().pipelineId, equalTo( new PipelineId( REGION_ID, 2 ) ) );
        assertTrue( newPipelineReplicas[ 2 ].getQueue() instanceof EmptyOperatorQueue );
        assertThat( ( (DefaultOperatorQueue) pipelineOperator2.getQueue() ).getThreadingPref(), equalTo( MULTI_THREADED ) );
        assertTrue( pipelineOperator2.getDrainerPool() instanceof BlockingTupleQueueDrainerPool );
        assertThat( newPipelineReplicas[ 3 ].id().pipelineId, equalTo( new PipelineId( REGION_ID, 3 ) ) );
        assertTrue( newPipelineReplicas[ 3 ].getQueue() instanceof EmptyOperatorQueue );
        assertThat( ( (DefaultOperatorQueue) pipelineOperator3.getQueue() ).getThreadingPref(), equalTo( MULTI_THREADED ) );
        assertTrue( pipelineOperator3.getDrainerPool() instanceof BlockingTupleQueueDrainerPool );
        assertThat( newPipelineReplicas[ 4 ].id().pipelineId, equalTo( new PipelineId( REGION_ID, 4 ) ) );
        assertTrue( newPipelineReplicas[ 4 ].getQueue() instanceof EmptyOperatorQueue );
        assertThat( ( (DefaultOperatorQueue) pipelineOperator4.getQueue() ).getThreadingPref(), equalTo( MULTI_THREADED ) );
        assertTrue( pipelineOperator4.getDrainerPool() instanceof BlockingTupleQueueDrainerPool );

        assertNull( pipelineOperator0.getDownstreamContext() );
        assertNull( pipelineOperator1.getDownstreamContext() );
        assertNull( pipelineOperator2.getDownstreamContext() );
        assertNull( pipelineOperator3.getDownstreamContext() );
        assertNull( pipelineOperator4.getDownstreamContext() );

        for ( PipelineReplica newPipelineReplica : newPipelineReplicas )
        {
            final PipelineReplicaMeter meter = newPipelineReplica.getMeter();
            final OperatorReplica operator = newPipelineReplica.getOperatorReplica( 0 );
            assertThat( meter.getHeadOperatorId(), equalTo( operator.getOperatorDef( 0 ).getId() ) );
            assertThat( meter.getInputPortCount(), equalTo( operator.getOperatorDef( 0 ).getInputPortCount() ) );
            assertPipelineReplicaMeter( newPipelineReplica );
        }
    }

    @Test
    public void shouldCopyNonSplitPipelinesAtHeadOfTheRegion ()
    {
        final OperatorDef operatorDef0 = OperatorDefBuilder.newInstance( "op0", StatefulInput0Output1Operator.class ).build();
        final OperatorDef operatorDef1 = OperatorDefBuilder.newInstance( "op1", PartitionedStatefulInput1Output1Operator.class )
                                                           .setPartitionFieldNames( singletonList( "field" ) )
                                                           .build();
        final OperatorDef operatorDef2 = OperatorDefBuilder.newInstance( "op2", StatelessInput1Output1Operator.class ).build();
        final OperatorDef operatorDef3 = OperatorDefBuilder.newInstance( "op3", StatelessInput1Output1Operator.class ).build();
        final OperatorDef operatorDef4 = OperatorDefBuilder.newInstance( "op4", StatelessInput1Output1Operator.class ).build();
        final OperatorDef operatorDef5 = OperatorDefBuilder.newInstance( "op5", StatelessInput1Output1Operator.class ).build();
        final OperatorDef operatorDef6 = OperatorDefBuilder.newInstance( "op6", StatelessInput1Output1Operator.class ).build();

        final FlowDef flow = new FlowDefBuilder().add( operatorDef0 )
                                                 .add( operatorDef1 )
                                                 .add( operatorDef2 )
                                                 .add( operatorDef3 )
                                                 .add( operatorDef4 )
                                                 .add( operatorDef5 )
                                                 .add( operatorDef6 )
                                                 .connect( "op0", "op1" )
                                                 .connect( "op1", "op2" )
                                                 .connect( "op2", "op3" )
                                                 .connect( "op3", "op4" )
                                                 .connect( "op4", "op5" )
                                                 .connect( "op5", "op6" )
                                                 .build();

        final RegionDef regionDef = new RegionDef( REGION_ID,
                                                   PARTITIONED_STATEFUL,
                                                   singletonList( "field" ),
                                                   asList( operatorDef1,
                                                           operatorDef2,
                                                           operatorDef3,
                                                           operatorDef4,
                                                           operatorDef5,
                                                           operatorDef6 ) );

        final RegionExecPlan regionExecPlan = new RegionExecPlan( regionDef, asList( 0, 1, 2 ), 2 );
        final Region region = regionManager.createRegion( flow, regionExecPlan );
        initialize( region );

        final Region newRegion = pipelineTransformer.splitPipeline( region, asList( 2, 4 ) );

        assertThat( newRegion.getPipelineReplicas( 0 ), equalTo( region.getPipelineReplicas( 0 ) ) );
        assertThat( newRegion.getPipelineReplicas( 1 ), equalTo( region.getPipelineReplicas( 1 ) ) );
    }

    @Test
    public void shouldCopyNonSplitPipelinesAtTailOfTheRegion ()
    {
        final OperatorDef operatorDef0 = OperatorDefBuilder.newInstance( "op0", StatefulInput0Output1Operator.class ).build();
        final OperatorDef operatorDef1 = OperatorDefBuilder.newInstance( "op1", PartitionedStatefulInput1Output1Operator.class )
                                                           .setPartitionFieldNames( singletonList( "field" ) )
                                                           .build();
        final OperatorDef operatorDef2 = OperatorDefBuilder.newInstance( "op2", StatelessInput1Output1Operator.class ).build();
        final OperatorDef operatorDef3 = OperatorDefBuilder.newInstance( "op3", StatelessInput1Output1Operator.class ).build();
        final OperatorDef operatorDef4 = OperatorDefBuilder.newInstance( "op4", StatelessInput1Output1Operator.class ).build();
        final OperatorDef operatorDef5 = OperatorDefBuilder.newInstance( "op5", StatelessInput1Output1Operator.class ).build();
        final OperatorDef operatorDef6 = OperatorDefBuilder.newInstance( "op6", StatelessInput1Output1Operator.class ).build();

        final FlowDef flow = new FlowDefBuilder().add( operatorDef0 )
                                                 .add( operatorDef1 )
                                                 .add( operatorDef2 )
                                                 .add( operatorDef3 )
                                                 .add( operatorDef4 )
                                                 .add( operatorDef5 )
                                                 .add( operatorDef6 )
                                                 .connect( "op0", "op1" )
                                                 .connect( "op1", "op2" )
                                                 .connect( "op2", "op3" )
                                                 .connect( "op3", "op4" )
                                                 .connect( "op4", "op5" )
                                                 .connect( "op5", "op6" )
                                                 .build();

        final RegionDef regionDef = new RegionDef( REGION_ID,
                                                   PARTITIONED_STATEFUL,
                                                   singletonList( "field" ),
                                                   asList( operatorDef1,
                                                           operatorDef2,
                                                           operatorDef3,
                                                           operatorDef4,
                                                           operatorDef5,
                                                           operatorDef6 ) );

        final RegionExecPlan regionExecPlan = new RegionExecPlan( regionDef, asList( 0, 3, 4 ), 2 );
        final Region region = regionManager.createRegion( flow, regionExecPlan );
        initialize( region );

        final Region newRegion = pipelineTransformer.splitPipeline( region, asList( 0, 1, 2 ) );

        assertThat( newRegion.getPipelineReplicas( 3 ), equalTo( region.getPipelineReplicas( 1 ) ) );
        assertThat( newRegion.getPipelineReplicas( 4 ), equalTo( region.getPipelineReplicas( 2 ) ) );
    }

    @Test
    public void testCheckPipelineStartIndicesToSplit ()
    {
        final OperatorDef operatorDef0 = OperatorDefBuilder.newInstance( "op0", PartitionedStatefulInput1Output1Operator.class )
                                                           .setPartitionFieldNames( singletonList( "field" ) )
                                                           .build();
        final OperatorDef operatorDef1 = OperatorDefBuilder.newInstance( "op1", StatelessInput1Output1Operator.class ).build();
        final OperatorDef operatorDef2 = OperatorDefBuilder.newInstance( "op2", StatelessInput1Output1Operator.class ).build();
        final OperatorDef operatorDef3 = OperatorDefBuilder.newInstance( "op3", StatelessInput1Output1Operator.class ).build();
        final OperatorDef operatorDef4 = OperatorDefBuilder.newInstance( "op4", StatelessInput1Output1Operator.class ).build();
        final OperatorDef operatorDef5 = OperatorDefBuilder.newInstance( "op5", StatelessInput1Output1Operator.class ).build();
        final OperatorDef operatorDef6 = OperatorDefBuilder.newInstance( "op6", StatelessInput1Output1Operator.class ).build();

        final RegionDef regionDef = new RegionDef( REGION_ID, PARTITIONED_STATEFUL, singletonList( "field" ),
                                                   asList( operatorDef0,
                                                           operatorDef1,
                                                           operatorDef2,
                                                           operatorDef3,
                                                           operatorDef4,
                                                           operatorDef5,
                                                           operatorDef6 ) );

        final RegionExecPlan regionExecPlan = new RegionExecPlan( regionDef, asList( 0, 2, 4 ), 2 );

        assertFalse( checkPipelineStartIndicesToSplit( regionExecPlan, singletonList( 0 ) ) );
        assertFalse( checkPipelineStartIndicesToSplit( regionExecPlan, singletonList( -1 ) ) );
        assertFalse( checkPipelineStartIndicesToSplit( regionExecPlan, asList( 0, 1, 2 ) ) );
        assertFalse( checkPipelineStartIndicesToSplit( regionExecPlan, asList( 4, 7 ) ) );

        assertTrue( checkPipelineStartIndicesToSplit( regionExecPlan, asList( 0, 1 ) ) );
        assertTrue( checkPipelineStartIndicesToSplit( regionExecPlan, asList( 4, 5 ) ) );
        assertTrue( checkPipelineStartIndicesToSplit( regionExecPlan, asList( 4, 5, 6 ) ) );
    }

    private void initialize ( final Region region )
    {
        for ( int replicaIndex = 0; replicaIndex < region.getExecPlan().getReplicaCount(); replicaIndex++ )
        {
            for ( PipelineReplica pipelineReplica : region.getReplicaPipelines( replicaIndex ) )
            {
                final PipelineId pipelineId = pipelineReplica.id().pipelineId;
                pipelineReplica.init( region.getFusedSchedulingStrategies( pipelineId ), region.getFusedUpstreamContexts( pipelineId ) );
            }
        }
    }

    private Tuple newTuple ( final String key, final Object value )
    {
        final Tuple tuple = new Tuple();
        tuple.set( key, value );
        return tuple;
    }

    @OperatorSpec( type = STATELESS, inputPortCount = 0, outputPortCount = 1 )
    @OperatorSchema( outputs = { @PortSchema( portIndex = 0, scope = EXACT_FIELD_SET, fields = { @SchemaField( name = "field", type = Integer.class ) } ) } )
    public static class StatelessInput0Output1Operator extends NopOperator
    {

        @Override
        public SchedulingStrategy init ( final InitializationContext ctx )
        {
            return ScheduleWhenAvailable.INSTANCE;
        }

    }


    @OperatorSpec( type = STATELESS, inputPortCount = 1, outputPortCount = 1 )
    @OperatorSchema( inputs = { @PortSchema( portIndex = 0, scope = EXACT_FIELD_SET, fields = { @SchemaField( name = "field", type = Integer.class ) } ) }, outputs = {
            @PortSchema( portIndex = 0, scope = EXACT_FIELD_SET, fields = { @SchemaField( name = "field", type = Integer.class ) } ) } )
    public static class StatelessInput1Output1Operator extends NopOperator
    {

        @Override
        public SchedulingStrategy init ( final InitializationContext ctx )
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
        public SchedulingStrategy init ( final InitializationContext ctx )
        {
            return scheduleWhenTuplesAvailableOnDefaultPort( 1 );
        }

    }


    @OperatorSpec( type = STATEFUL, inputPortCount = 1, outputPortCount = 1 )
    @OperatorSchema( inputs = { @PortSchema( portIndex = 0, scope = EXACT_FIELD_SET, fields = { @SchemaField( name = "field", type = Integer.class ) } ) }, outputs = {
            @PortSchema( portIndex = 0, scope = EXACT_FIELD_SET, fields = { @SchemaField( name = "field", type = Integer.class ) } ) } )
    public static class FusibleStatefulInput1Output1Operator extends NopOperator
    {

        @Override
        public SchedulingStrategy init ( final InitializationContext ctx )
        {
            return scheduleWhenTuplesAvailableOnDefaultPort( 1 );
        }

    }


    @OperatorSpec( type = STATEFUL, inputPortCount = 1, outputPortCount = 1 )
    @OperatorSchema( inputs = { @PortSchema( portIndex = 0, scope = EXACT_FIELD_SET, fields = { @SchemaField( name = "field", type = Integer.class ) } ) }, outputs = {
            @PortSchema( portIndex = 0, scope = EXACT_FIELD_SET, fields = { @SchemaField( name = "field", type = Integer.class ) } ) } )
    public static class NonFusibleStatefulInput1Output1Operator extends NopOperator
    {

        @Override
        public SchedulingStrategy init ( final InitializationContext ctx )
        {
            return scheduleWhenTuplesAvailableOnDefaultPort( 2 );
        }

    }


    @OperatorSpec( type = STATEFUL, inputPortCount = 0, outputPortCount = 1 )
    @OperatorSchema( outputs = { @PortSchema( portIndex = 0, scope = EXACT_FIELD_SET, fields = { @SchemaField( name = "field", type = Integer.class ) } ) } )
    public static class StatefulInput0Output1Operator extends NopOperator
    {

        @Override
        public SchedulingStrategy init ( final InitializationContext ctx )
        {
            return scheduleWhenTuplesAvailableOnDefaultPort( 1 );
        }

    }


    public static abstract class NopOperator implements Operator
    {

        @Override
        public void invoke ( final InvocationContext ctx )
        {

        }

    }

}
