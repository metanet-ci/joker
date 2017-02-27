package cs.bilkent.joker.engine.region.impl;

import java.util.List;

import org.junit.Test;

import cs.bilkent.joker.engine.config.JokerConfig;
import static cs.bilkent.joker.engine.config.ThreadingPreference.MULTI_THREADED;
import static cs.bilkent.joker.engine.config.ThreadingPreference.SINGLE_THREADED;
import cs.bilkent.joker.engine.flow.PipelineId;
import cs.bilkent.joker.engine.flow.RegionDef;
import cs.bilkent.joker.engine.flow.RegionExecutionPlan;
import cs.bilkent.joker.engine.kvstore.impl.OperatorKVStoreManagerImpl;
import cs.bilkent.joker.engine.metric.PipelineReplicaMeter;
import cs.bilkent.joker.engine.partition.PartitionKeyExtractorFactory;
import cs.bilkent.joker.engine.partition.PartitionService;
import cs.bilkent.joker.engine.partition.impl.PartitionKeyExtractorFactoryImpl;
import cs.bilkent.joker.engine.partition.impl.PartitionServiceImpl;
import cs.bilkent.joker.engine.pipeline.OperatorReplica;
import cs.bilkent.joker.engine.pipeline.PipelineReplica;
import cs.bilkent.joker.engine.pipeline.UpstreamConnectionStatus;
import cs.bilkent.joker.engine.pipeline.UpstreamContext;
import cs.bilkent.joker.engine.pipeline.impl.tuplesupplier.CachedTuplesImplSupplier;
import cs.bilkent.joker.engine.region.PipelineTransformer;
import cs.bilkent.joker.engine.region.Region;
import static cs.bilkent.joker.engine.region.impl.RegionManagerImplTest.assertPipelineReplicaMeter;
import cs.bilkent.joker.engine.tuplequeue.impl.OperatorTupleQueueManagerImpl;
import cs.bilkent.joker.engine.tuplequeue.impl.drainer.GreedyDrainer;
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
import cs.bilkent.joker.operator.Tuple;
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
import static java.util.Arrays.fill;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class PipelineTransformerImplTest extends AbstractJokerTest
{

    private static final int REGION_ID = 1;

    private final JokerConfig config = new JokerConfig();

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
    public void shouldMergeAllPipelinesOfStatefulRegion ()
    {
        final OperatorDef operatorDef0 = OperatorDefBuilder.newInstance( "op0", StatelessInput0Output1Operator.class ).build();
        final OperatorDef operatorDef1 = OperatorDefBuilder.newInstance( "op1", StatefulInput1Output1Operator.class ).build();
        final OperatorDef operatorDef2 = OperatorDefBuilder.newInstance( "op2", StatelessInput1Output1Operator.class ).build();
        final OperatorDef operatorDef3 = OperatorDefBuilder.newInstance( "op3", StatefulInput1Output1Operator.class ).build();
        final OperatorDef operatorDef4 = OperatorDefBuilder.newInstance( "op4", StatelessInput1Output1Operator.class ).build();
        final OperatorDef operatorDef5 = OperatorDefBuilder.newInstance( "op5", StatefulInput1Output1Operator.class ).build();

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

        final RegionExecutionPlan regionExecutionPlan = new RegionExecutionPlan( regionDef, asList( 0, 2, 4 ), 1 );
        final Region region = regionManager.createRegion( flow, regionExecutionPlan );
        initialize( region );

        final PipelineReplica[] pipelineReplicas = region.getReplicaPipelines( 0 );
        pipelineReplicas[ 0 ].getOperator( 0 ).getQueue().offer( 0, singletonList( newTuple( "key0", "val0" ) ) );
        pipelineReplicas[ 0 ].getOperator( 0 ).getOperatorKvStore().getKVStore( null ).set( "key0", "val0" );
        pipelineReplicas[ 0 ].getOperator( 1 ).getQueue().offer( 0, singletonList( newTuple( "key1", "val1" ) ) );
        pipelineReplicas[ 1 ].getOperator( 0 ).getQueue().offer( 0, singletonList( newTuple( "key2", "val2" ) ) );
        pipelineReplicas[ 1 ].getOperator( 0 ).getOperatorKvStore().getKVStore( null ).set( "key2", "val2" );
        pipelineReplicas[ 1 ].getOperator( 1 ).getQueue().offer( 0, singletonList( newTuple( "key3", "val3" ) ) );
        pipelineReplicas[ 2 ].getOperator( 0 ).getQueue().offer( 0, singletonList( newTuple( "key4", "val4" ) ) );
        pipelineReplicas[ 2 ].getOperator( 0 ).getOperatorKvStore().getKVStore( null ).set( "key4", "val4" );

        final PipelineReplicaMeter pipelineReplicaMeter = pipelineReplicas[ 0 ].getMeter();

        final Region newRegion = pipelineTransformer.mergePipelines( region, asList( 0, 2, 4 ) );

        assertThat( newRegion.getExecutionPlan().getReplicaCount(), equalTo( 1 ) );
        assertThat( newRegion.getExecutionPlan().getPipelineStartIndices(), equalTo( singletonList( 0 ) ) );

        final PipelineReplica[] newPipelineReplicas = newRegion.getReplicaPipelines( 0 );
        assertThat( newPipelineReplicas.length, equalTo( 1 ) );
        final PipelineReplica newPipelineReplica = newPipelineReplicas[ 0 ];
        final OperatorReplica pipelineOperator0 = newPipelineReplica.getOperator( 0 );
        final OperatorReplica pipelineOperator1 = newPipelineReplica.getOperator( 1 );
        final OperatorReplica pipelineOperator2 = newPipelineReplica.getOperator( 2 );
        final OperatorReplica pipelineOperator3 = newPipelineReplica.getOperator( 3 );
        final OperatorReplica pipelineOperator4 = newPipelineReplica.getOperator( 4 );

        assertThat( singletonList( newTuple( "key0", "val0" ) ), equalTo( drainDefaultPortGreedily( pipelineOperator0 ) ) );
        assertThat( singletonList( newTuple( "key1", "val1" ) ), equalTo( drainDefaultPortGreedily( pipelineOperator1 ) ) );
        assertThat( singletonList( newTuple( "key2", "val2" ) ), equalTo( drainDefaultPortGreedily( pipelineOperator2 ) ) );
        assertThat( singletonList( newTuple( "key3", "val3" ) ), equalTo( drainDefaultPortGreedily( pipelineOperator3 ) ) );
        assertThat( singletonList( newTuple( "key4", "val4" ) ), equalTo( drainDefaultPortGreedily( pipelineOperator4 ) ) );

        assertThat( ( (DefaultOperatorTupleQueue) pipelineOperator0.getQueue() ).getThreadingPreference(), equalTo( MULTI_THREADED ) );
        assertTrue( pipelineOperator0.getDrainerPool() instanceof BlockingTupleQueueDrainerPool );
        assertThat( pipelineOperator0.getOperatorKvStore().getKVStore( null ).get( "key0" ), equalTo( "val0" ) );
        assertThat( ( (DefaultOperatorTupleQueue) pipelineOperator1.getQueue() ).getThreadingPreference(), equalTo( SINGLE_THREADED ) );
        assertTrue( pipelineOperator1.getDrainerPool() instanceof NonBlockingTupleQueueDrainerPool );
        assertThat( ( (DefaultOperatorTupleQueue) pipelineOperator2.getQueue() ).getThreadingPreference(), equalTo( SINGLE_THREADED ) );
        assertTrue( pipelineOperator2.getDrainerPool() instanceof NonBlockingTupleQueueDrainerPool );
        assertThat( pipelineOperator2.getOperatorKvStore().getKVStore( null ).get( "key2" ), equalTo( "val2" ) );
        assertThat( ( (DefaultOperatorTupleQueue) pipelineOperator3.getQueue() ).getThreadingPreference(), equalTo( SINGLE_THREADED ) );
        assertTrue( pipelineOperator3.getDrainerPool() instanceof NonBlockingTupleQueueDrainerPool );
        assertThat( ( (DefaultOperatorTupleQueue) pipelineOperator4.getQueue() ).getThreadingPreference(), equalTo( SINGLE_THREADED ) );
        assertTrue( pipelineOperator4.getDrainerPool() instanceof NonBlockingTupleQueueDrainerPool );
        assertThat( pipelineOperator4.getOperatorKvStore().getKVStore( null ).get( "key4" ), equalTo( "val4" ) );

        final PipelineReplicaMeter meter = newPipelineReplica.getMeter();
        assertThat( meter.getHeadOperatorId(), equalTo( pipelineOperator0.getOperatorDef().getId() ) );
        assertThat( meter.getConsumedPortCount(), equalTo( pipelineOperator0.getOperatorDef().getInputPortCount() ) );
        assertThat( meter.getTailOperatorId(), equalTo( pipelineOperator4.getOperatorDef().getId() ) );
        assertThat( meter.getProducedPortCount(), equalTo( pipelineOperator4.getOperatorDef().getOutputPortCount() ) );
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

        final RegionExecutionPlan regionExecutionPlan = new RegionExecutionPlan( regionDef, asList( 0, 2, 4 ), 1 );
        final Region region = regionManager.createRegion( flow, regionExecutionPlan );
        initialize( region );

        final PipelineReplica[] pipelineReplicas = region.getReplicaPipelines( 0 );
        pipelineReplicas[ 0 ].getSelfPipelineTupleQueue().offer( 0, singletonList( newTuple( "field", "val0" ) ) );
        pipelineReplicas[ 0 ].getOperator( 1 ).getQueue().offer( 0, singletonList( newTuple( "field", "val1" ) ) );
        pipelineReplicas[ 1 ].getSelfPipelineTupleQueue().offer( 0, singletonList( newTuple( "field", "val2" ) ) );
        pipelineReplicas[ 1 ].getOperator( 1 ).getQueue().offer( 0, singletonList( newTuple( "field", "val3" ) ) );
        pipelineReplicas[ 2 ].getSelfPipelineTupleQueue().offer( 0, singletonList( newTuple( "field", "val4" ) ) );

        final Region newRegion = pipelineTransformer.mergePipelines( region, asList( 0, 2, 4 ) );

        assertThat( newRegion.getExecutionPlan().getReplicaCount(), equalTo( 1 ) );
        assertThat( newRegion.getExecutionPlan().getPipelineStartIndices(), equalTo( singletonList( 0 ) ) );

        final PipelineReplica[] newPipelineReplicas = newRegion.getReplicaPipelines( 0 );
        assertThat( newPipelineReplicas.length, equalTo( 1 ) );
        final PipelineReplica newPipelineReplica = newPipelineReplicas[ 0 ];
        final OperatorReplica pipelineOperator1 = newPipelineReplica.getOperator( 1 );
        final OperatorReplica pipelineOperator2 = newPipelineReplica.getOperator( 2 );
        final OperatorReplica pipelineOperator3 = newPipelineReplica.getOperator( 3 );
        final OperatorReplica pipelineOperator4 = newPipelineReplica.getOperator( 4 );

        final GreedyDrainer drainer = new GreedyDrainer( operatorDef1.getInputPortCount() );
        newPipelineReplica.getSelfPipelineTupleQueue().drain( drainer );
        assertThat( singletonList( newTuple( "field", "val0" ) ), equalTo( drainer.getResult().getTuples( 0 ) ) );

        assertThat( singletonList( newTuple( "field", "val1" ) ), equalTo( drainDefaultPortGreedily( pipelineOperator1 ) ) );
        assertThat( singletonList( newTuple( "field", "val2" ) ), equalTo( drainDefaultPortGreedily( pipelineOperator2 ) ) );
        assertThat( singletonList( newTuple( "field", "val3" ) ), equalTo( drainDefaultPortGreedily( pipelineOperator3 ) ) );
        assertThat( singletonList( newTuple( "field", "val4" ) ), equalTo( drainDefaultPortGreedily( pipelineOperator4 ) ) );

        final PipelineReplicaMeter pipelineReplicaMeter = newPipelineReplica.getMeter();
        final OperatorDef[] mergedOperatorDefs = newRegion.getExecutionPlan().getOperatorDefsByPipelineStartIndex( 0 );

        assertThat( pipelineReplicaMeter.getHeadOperatorId(), equalTo( mergedOperatorDefs[ 0 ].getId() ) );
        assertThat( pipelineReplicaMeter.getConsumedPortCount(), equalTo( mergedOperatorDefs[ 0 ].getInputPortCount() ) );
        assertThat( pipelineReplicaMeter.getTailOperatorId(), equalTo( mergedOperatorDefs[ mergedOperatorDefs.length - 1 ].getId() ) );
        assertThat( pipelineReplicaMeter.getProducedPortCount(),
                    equalTo( mergedOperatorDefs[ mergedOperatorDefs.length - 1 ].getOutputPortCount() ) );

        assertPipelineReplicaMeter( newPipelineReplica );
    }

    @Test
    public void shouldMergeSingleOperatorPipelines ()
    {
        final OperatorDef operatorDef0 = OperatorDefBuilder.newInstance( "op0", PartitionedStatefulInput1Output1Operator.class )
                                                           .setPartitionFieldNames( singletonList( "field" ) )
                                                           .build();
        final OperatorDef operatorDef1 = OperatorDefBuilder.newInstance( "op1", StatelessInput1Output1Operator.class ).build();
        final OperatorDef operatorDef2 = OperatorDefBuilder.newInstance( "op2", StatelessInput1Output1Operator.class ).build();
        final OperatorDef operatorDef3 = OperatorDefBuilder.newInstance( "op3", StatelessInput1Output1Operator.class ).build();
        final OperatorDef operatorDef4 = OperatorDefBuilder.newInstance( "op4", StatelessInput1Output1Operator.class ).build();
        final OperatorDef operatorDef5 = OperatorDefBuilder.newInstance( "op5", StatelessInput1Output1Operator.class ).build();

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

        final RegionDef regionDef = new RegionDef( REGION_ID, STATELESS, emptyList(),
                                                   asList( operatorDef1, operatorDef2, operatorDef3, operatorDef4, operatorDef5 ) );

        final RegionExecutionPlan regionExecutionPlan = new RegionExecutionPlan( regionDef, asList( 0, 1, 2, 3, 4 ), 1 );
        final Region region = regionManager.createRegion( flow, regionExecutionPlan );
        initialize( region );

        final Region newRegion = pipelineTransformer.mergePipelines( region, asList( 0, 1, 2, 3, 4 ) );

        final PipelineReplica[] newPipelineReplicas = newRegion.getReplicaPipelines( 0 );
        assertThat( newPipelineReplicas.length, equalTo( 1 ) );
        final PipelineReplica newPipelineReplica = newPipelineReplicas[ 0 ];
        final OperatorReplica pipelineOperator0 = newPipelineReplica.getOperator( 0 );
        final OperatorReplica pipelineOperator1 = newPipelineReplica.getOperator( 1 );
        final OperatorReplica pipelineOperator2 = newPipelineReplica.getOperator( 2 );
        final OperatorReplica pipelineOperator3 = newPipelineReplica.getOperator( 3 );
        final OperatorReplica pipelineOperator4 = newPipelineReplica.getOperator( 4 );
        final OperatorReplica pipelineOperator5 = newPipelineReplica.getOperator( 4 );

        assertThat( ( (DefaultOperatorTupleQueue) pipelineOperator0.getQueue() ).getThreadingPreference(), equalTo( MULTI_THREADED ) );
        assertTrue( pipelineOperator0.getDrainerPool() instanceof BlockingTupleQueueDrainerPool );
        assertTrue( pipelineOperator0.getOutputSupplier() instanceof CachedTuplesImplSupplier );
        assertThat( ( (DefaultOperatorTupleQueue) pipelineOperator1.getQueue() ).getThreadingPreference(), equalTo( SINGLE_THREADED ) );
        assertTrue( pipelineOperator1.getDrainerPool() instanceof NonBlockingTupleQueueDrainerPool );
        assertTrue( pipelineOperator1.getOutputSupplier() instanceof CachedTuplesImplSupplier );
        assertThat( ( (DefaultOperatorTupleQueue) pipelineOperator2.getQueue() ).getThreadingPreference(), equalTo( SINGLE_THREADED ) );
        assertTrue( pipelineOperator2.getDrainerPool() instanceof NonBlockingTupleQueueDrainerPool );
        assertTrue( pipelineOperator2.getOutputSupplier() instanceof CachedTuplesImplSupplier );
        assertThat( ( (DefaultOperatorTupleQueue) pipelineOperator3.getQueue() ).getThreadingPreference(), equalTo( SINGLE_THREADED ) );
        assertTrue( pipelineOperator3.getDrainerPool() instanceof NonBlockingTupleQueueDrainerPool );
        assertTrue( pipelineOperator3.getOutputSupplier() instanceof CachedTuplesImplSupplier );
        assertThat( ( (DefaultOperatorTupleQueue) pipelineOperator4.getQueue() ).getThreadingPreference(), equalTo( SINGLE_THREADED ) );
        assertTrue( pipelineOperator4.getDrainerPool() instanceof NonBlockingTupleQueueDrainerPool );
        assertThat( ( (DefaultOperatorTupleQueue) pipelineOperator5.getQueue() ).getThreadingPreference(), equalTo( SINGLE_THREADED ) );
        assertTrue( pipelineOperator5.getDrainerPool() instanceof NonBlockingTupleQueueDrainerPool );

        final PipelineReplicaMeter meter = newPipelineReplica.getMeter();
        assertThat( meter.getHeadOperatorId(), equalTo( pipelineOperator0.getOperatorDef().getId() ) );
        assertThat( meter.getConsumedPortCount(), equalTo( pipelineOperator0.getOperatorDef().getInputPortCount() ) );
        assertThat( meter.getTailOperatorId(), equalTo( pipelineOperator4.getOperatorDef().getId() ) );
        assertThat( meter.getProducedPortCount(), equalTo( pipelineOperator4.getOperatorDef().getOutputPortCount() ) );
        assertPipelineReplicaMeter( newPipelineReplica );
    }

    @Test
    public void shouldCopyNonMergedPipelinesAtHeadOfTheRegion ()
    {
        final OperatorDef operatorDef0 = OperatorDefBuilder.newInstance( "op0", PartitionedStatefulInput1Output1Operator.class )
                                                           .setPartitionFieldNames( singletonList( "field" ) )
                                                           .build();
        final OperatorDef operatorDef1 = OperatorDefBuilder.newInstance( "op1", StatelessInput1Output1Operator.class ).build();
        final OperatorDef operatorDef2 = OperatorDefBuilder.newInstance( "op2", StatelessInput1Output1Operator.class ).build();
        final OperatorDef operatorDef3 = OperatorDefBuilder.newInstance( "op3", StatelessInput1Output1Operator.class ).build();
        final OperatorDef operatorDef4 = OperatorDefBuilder.newInstance( "op4", StatelessInput1Output1Operator.class ).build();
        final OperatorDef operatorDef5 = OperatorDefBuilder.newInstance( "op5", StatelessInput1Output1Operator.class ).build();

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
                                                   asList( operatorDef0,
                                                           operatorDef1,
                                                           operatorDef2,
                                                           operatorDef3,
                                                           operatorDef4,
                                                           operatorDef5 ) );

        final RegionExecutionPlan regionExecutionPlan = new RegionExecutionPlan( regionDef, asList( 0, 2, 3, 4 ), 2 );
        final Region region = regionManager.createRegion( flow, regionExecutionPlan );
        initialize( region );

        final Region newRegion = pipelineTransformer.mergePipelines( region, asList( 3, 4 ) );

        assertThat( newRegion.getPipelineReplicas( 0 ), equalTo( region.getPipelineReplicas( 0 ) ) );
        assertThat( newRegion.getPipelineReplicas( 1 ), equalTo( region.getPipelineReplicas( 1 ) ) );
    }

    @Test
    public void shouldCopyNonMergedPipelinesAtTailOfTheRegion ()
    {
        final OperatorDef operatorDef0 = OperatorDefBuilder.newInstance( "op0", PartitionedStatefulInput1Output1Operator.class )
                                                           .setPartitionFieldNames( singletonList( "field" ) )
                                                           .build();
        final OperatorDef operatorDef1 = OperatorDefBuilder.newInstance( "op1", StatelessInput1Output1Operator.class ).build();
        final OperatorDef operatorDef2 = OperatorDefBuilder.newInstance( "op2", StatelessInput1Output1Operator.class ).build();
        final OperatorDef operatorDef3 = OperatorDefBuilder.newInstance( "op3", StatelessInput1Output1Operator.class ).build();
        final OperatorDef operatorDef4 = OperatorDefBuilder.newInstance( "op4", StatelessInput1Output1Operator.class ).build();
        final OperatorDef operatorDef5 = OperatorDefBuilder.newInstance( "op5", StatelessInput1Output1Operator.class ).build();

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
                                                   asList( operatorDef0,
                                                           operatorDef1,
                                                           operatorDef2,
                                                           operatorDef3,
                                                           operatorDef4,
                                                           operatorDef5 ) );

        final RegionExecutionPlan regionExecutionPlan = new RegionExecutionPlan( regionDef, asList( 0, 2, 3, 4 ), 2 );
        final Region region = regionManager.createRegion( flow, regionExecutionPlan );
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

        final RegionExecutionPlan regionExecutionPlan = new RegionExecutionPlan( regionDef, asList( 0, 2, 3, 4 ), 2 );

        assertFalse( pipelineTransformer.checkPipelineStartIndicesToMerge( regionExecutionPlan, singletonList( 0 ) ) );
        assertFalse( pipelineTransformer.checkPipelineStartIndicesToMerge( regionExecutionPlan, singletonList( -1 ) ) );
        assertFalse( pipelineTransformer.checkPipelineStartIndicesToMerge( regionExecutionPlan, asList( 0, 2, 4 ) ) );

        assertTrue( pipelineTransformer.checkPipelineStartIndicesToMerge( regionExecutionPlan, asList( 0, 2 ) ) );
        assertTrue( pipelineTransformer.checkPipelineStartIndicesToMerge( regionExecutionPlan, asList( 0, 2, 3 ) ) );
        assertTrue( pipelineTransformer.checkPipelineStartIndicesToMerge( regionExecutionPlan, asList( 0, 2, 3, 4 ) ) );
        assertTrue( pipelineTransformer.checkPipelineStartIndicesToMerge( regionExecutionPlan, asList( 3, 4 ) ) );
    }

    @Test
    public void shouldSplitSinglePipelineOfStatefulRegion ()
    {

        final OperatorDef operatorDef0 = OperatorDefBuilder.newInstance( "op0", StatelessInput0Output1Operator.class ).build();
        final OperatorDef operatorDef1 = OperatorDefBuilder.newInstance( "op1", StatefulInput1Output1Operator.class ).build();
        final OperatorDef operatorDef2 = OperatorDefBuilder.newInstance( "op2", StatelessInput1Output1Operator.class ).build();
        final OperatorDef operatorDef3 = OperatorDefBuilder.newInstance( "op3", StatefulInput1Output1Operator.class ).build();
        final OperatorDef operatorDef4 = OperatorDefBuilder.newInstance( "op4", StatelessInput1Output1Operator.class ).build();
        final OperatorDef operatorDef5 = OperatorDefBuilder.newInstance( "op5", StatefulInput1Output1Operator.class ).build();

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

        final RegionExecutionPlan regionExecutionPlan = new RegionExecutionPlan( regionDef, singletonList( 0 ), 1 );
        final Region region = regionManager.createRegion( flow, regionExecutionPlan );
        initialize( region );

        final PipelineReplica[] pipelineReplicas = region.getReplicaPipelines( 0 );
        pipelineReplicas[ 0 ].getOperator( 0 ).getQueue().offer( 0, singletonList( newTuple( "key0", "val0" ) ) );
        pipelineReplicas[ 0 ].getOperator( 0 ).getOperatorKvStore().getKVStore( null ).set( "key0", "val0" );
        pipelineReplicas[ 0 ].getOperator( 1 ).getQueue().offer( 0, singletonList( newTuple( "key1", "val1" ) ) );
        pipelineReplicas[ 0 ].getOperator( 2 ).getQueue().offer( 0, singletonList( newTuple( "key2", "val2" ) ) );
        pipelineReplicas[ 0 ].getOperator( 2 ).getOperatorKvStore().getKVStore( null ).set( "key2", "val2" );
        pipelineReplicas[ 0 ].getOperator( 3 ).getQueue().offer( 0, singletonList( newTuple( "key3", "val3" ) ) );
        pipelineReplicas[ 0 ].getOperator( 4 ).getQueue().offer( 0, singletonList( newTuple( "key4", "val4" ) ) );
        pipelineReplicas[ 0 ].getOperator( 4 ).getOperatorKvStore().getKVStore( null ).set( "key4", "val4" );

        final Region newRegion = pipelineTransformer.splitPipeline( region, asList( 0, 2, 4 ) );

        assertThat( newRegion.getExecutionPlan().getReplicaCount(), equalTo( 1 ) );
        assertThat( newRegion.getExecutionPlan().getPipelineStartIndices(), equalTo( asList( 0, 2, 4 ) ) );

        final PipelineReplica[] newPipelineReplicas = newRegion.getReplicaPipelines( 0 );
        final OperatorReplica pipelineOperator0 = newPipelineReplicas[ 0 ].getOperator( 0 );
        final OperatorReplica pipelineOperator1 = newPipelineReplicas[ 0 ].getOperator( 1 );
        final OperatorReplica pipelineOperator2 = newPipelineReplicas[ 1 ].getOperator( 0 );
        final OperatorReplica pipelineOperator3 = newPipelineReplicas[ 1 ].getOperator( 1 );
        final OperatorReplica pipelineOperator4 = newPipelineReplicas[ 2 ].getOperator( 0 );

        assertThat( singletonList( newTuple( "key0", "val0" ) ), equalTo( drainDefaultPortGreedily( pipelineOperator0 ) ) );
        assertThat( singletonList( newTuple( "key1", "val1" ) ), equalTo( drainDefaultPortGreedily( pipelineOperator1 ) ) );
        assertThat( singletonList( newTuple( "key2", "val2" ) ), equalTo( drainDefaultPortGreedily( pipelineOperator2 ) ) );
        assertThat( singletonList( newTuple( "key3", "val3" ) ), equalTo( drainDefaultPortGreedily( pipelineOperator3 ) ) );
        assertThat( singletonList( newTuple( "key4", "val4" ) ), equalTo( drainDefaultPortGreedily( pipelineOperator4 ) ) );

        assertThat( newPipelineReplicas[ 0 ].id().pipelineId, equalTo( new PipelineId( REGION_ID, 0 ) ) );
        assertThat( ( (DefaultOperatorTupleQueue) pipelineOperator0.getQueue() ).getThreadingPreference(), equalTo( MULTI_THREADED ) );
        assertTrue( pipelineOperator0.getDrainerPool() instanceof BlockingTupleQueueDrainerPool );
        assertThat( pipelineOperator0.getOperatorKvStore().getKVStore( null ).get( "key0" ), equalTo( "val0" ) );
        assertThat( ( (DefaultOperatorTupleQueue) pipelineOperator1.getQueue() ).getThreadingPreference(), equalTo( SINGLE_THREADED ) );
        assertTrue( pipelineOperator1.getDrainerPool() instanceof NonBlockingTupleQueueDrainerPool );
        assertThat( newPipelineReplicas[ 1 ].id().pipelineId, equalTo( new PipelineId( REGION_ID, 2 ) ) );
        assertThat( ( (DefaultOperatorTupleQueue) pipelineOperator2.getQueue() ).getThreadingPreference(), equalTo( MULTI_THREADED ) );
        assertTrue( pipelineOperator2.getDrainerPool() instanceof BlockingTupleQueueDrainerPool );
        assertThat( pipelineOperator2.getOperatorKvStore().getKVStore( null ).get( "key2" ), equalTo( "val2" ) );
        assertThat( ( (DefaultOperatorTupleQueue) pipelineOperator3.getQueue() ).getThreadingPreference(), equalTo( SINGLE_THREADED ) );
        assertTrue( pipelineOperator3.getDrainerPool() instanceof NonBlockingTupleQueueDrainerPool );
        assertThat( newPipelineReplicas[ 2 ].id().pipelineId, equalTo( new PipelineId( REGION_ID, 4 ) ) );
        assertThat( ( (DefaultOperatorTupleQueue) pipelineOperator4.getQueue() ).getThreadingPreference(), equalTo( MULTI_THREADED ) );
        assertTrue( pipelineOperator4.getDrainerPool() instanceof BlockingTupleQueueDrainerPool );
        assertThat( pipelineOperator4.getOperatorKvStore().getKVStore( null ).get( "key4" ), equalTo( "val4" ) );

        final PipelineReplicaMeter meter0 = newPipelineReplicas[ 0 ].getMeter();
        assertThat( meter0.getHeadOperatorId(), equalTo( pipelineOperator0.getOperatorDef().getId() ) );
        assertThat( meter0.getConsumedPortCount(), equalTo( pipelineOperator0.getOperatorDef().getInputPortCount() ) );
        assertThat( meter0.getTailOperatorId(), equalTo( pipelineOperator1.getOperatorDef().getId() ) );
        assertThat( meter0.getProducedPortCount(), equalTo( pipelineOperator1.getOperatorDef().getOutputPortCount() ) );

        final PipelineReplicaMeter meter1 = newPipelineReplicas[ 1 ].getMeter();
        assertThat( meter1.getHeadOperatorId(), equalTo( pipelineOperator2.getOperatorDef().getId() ) );
        assertThat( meter1.getConsumedPortCount(), equalTo( pipelineOperator2.getOperatorDef().getInputPortCount() ) );
        assertThat( meter1.getTailOperatorId(), equalTo( pipelineOperator3.getOperatorDef().getId() ) );
        assertThat( meter1.getProducedPortCount(), equalTo( pipelineOperator3.getOperatorDef().getOutputPortCount() ) );

        final PipelineReplicaMeter meter2 = newPipelineReplicas[ 2 ].getMeter();
        assertThat( meter2.getHeadOperatorId(), equalTo( pipelineOperator4.getOperatorDef().getId() ) );
        assertThat( meter2.getConsumedPortCount(), equalTo( pipelineOperator4.getOperatorDef().getInputPortCount() ) );
        assertThat( meter2.getTailOperatorId(), equalTo( pipelineOperator4.getOperatorDef().getId() ) );
        assertThat( meter2.getProducedPortCount(), equalTo( pipelineOperator4.getOperatorDef().getOutputPortCount() ) );

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

        final RegionExecutionPlan regionExecutionPlan = new RegionExecutionPlan( regionDef, singletonList( 0 ), 1 );
        final Region region = regionManager.createRegion( flow, regionExecutionPlan );
        initialize( region );

        final PipelineReplica[] pipelineReplicas = region.getReplicaPipelines( 0 );
        pipelineReplicas[ 0 ].getSelfPipelineTupleQueue().offer( 0, singletonList( newTuple( "field", "val0" ) ) );
        pipelineReplicas[ 0 ].getOperator( 1 ).getQueue().offer( 0, singletonList( newTuple( "field", "val1" ) ) );
        pipelineReplicas[ 0 ].getOperator( 2 ).getQueue().offer( 0, singletonList( newTuple( "field", "val2" ) ) );
        pipelineReplicas[ 0 ].getOperator( 3 ).getQueue().offer( 0, singletonList( newTuple( "field", "val3" ) ) );
        pipelineReplicas[ 0 ].getOperator( 4 ).getQueue().offer( 0, singletonList( newTuple( "field", "val4" ) ) );

        final Region newRegion = pipelineTransformer.splitPipeline( region, asList( 0, 2, 4 ) );

        assertThat( newRegion.getExecutionPlan().getReplicaCount(), equalTo( 1 ) );
        assertThat( newRegion.getExecutionPlan().getPipelineStartIndices(), equalTo( asList( 0, 2, 4 ) ) );

        final PipelineReplica[] newPipelineReplicas = newRegion.getReplicaPipelines( 0 );
        final PipelineReplica newPipelineReplica = newPipelineReplicas[ 0 ];
        final OperatorReplica pipelineOperator0 = newPipelineReplicas[ 0 ].getOperator( 0 );
        final OperatorReplica pipelineOperator1 = newPipelineReplicas[ 0 ].getOperator( 1 );
        final OperatorReplica pipelineOperator2 = newPipelineReplicas[ 1 ].getOperator( 0 );
        final OperatorReplica pipelineOperator3 = newPipelineReplicas[ 1 ].getOperator( 1 );
        final OperatorReplica pipelineOperator4 = newPipelineReplicas[ 2 ].getOperator( 0 );

        final GreedyDrainer drainer = new GreedyDrainer( operatorDef1.getInputPortCount() );
        newPipelineReplica.getSelfPipelineTupleQueue().drain( drainer );
        assertThat( singletonList( newTuple( "field", "val0" ) ), equalTo( drainer.getResult().getTuples( 0 ) ) );

        assertThat( singletonList( newTuple( "field", "val1" ) ), equalTo( drainDefaultPortGreedily( pipelineOperator1 ) ) );
        assertThat( singletonList( newTuple( "field", "val2" ) ), equalTo( drainDefaultPortGreedily( pipelineOperator2 ) ) );
        assertThat( singletonList( newTuple( "field", "val3" ) ), equalTo( drainDefaultPortGreedily( pipelineOperator3 ) ) );
        assertThat( singletonList( newTuple( "field", "val4" ) ), equalTo( drainDefaultPortGreedily( pipelineOperator4 ) ) );

        assertThat( newPipelineReplicas[ 0 ].id().pipelineId, equalTo( new PipelineId( REGION_ID, 0 ) ) );
        assertTrue( newPipelineReplicas[ 0 ].getSelfPipelineTupleQueue() instanceof DefaultOperatorTupleQueue );
        assertTrue( pipelineOperator0.getQueue() instanceof PartitionedOperatorTupleQueue );
        assertTrue( pipelineOperator0.getDrainerPool() instanceof NonBlockingTupleQueueDrainerPool );
        assertThat( ( (DefaultOperatorTupleQueue) pipelineOperator1.getQueue() ).getThreadingPreference(), equalTo( SINGLE_THREADED ) );
        assertTrue( pipelineOperator1.getDrainerPool() instanceof NonBlockingTupleQueueDrainerPool );
        assertThat( newPipelineReplicas[ 1 ].id().pipelineId, equalTo( new PipelineId( REGION_ID, 2 ) ) );
        assertTrue( newPipelineReplicas[ 1 ].getSelfPipelineTupleQueue() instanceof DefaultOperatorTupleQueue );
        assertTrue( pipelineOperator2.getQueue() instanceof PartitionedOperatorTupleQueue );
        assertTrue( pipelineOperator2.getDrainerPool() instanceof NonBlockingTupleQueueDrainerPool );
        assertThat( ( (DefaultOperatorTupleQueue) pipelineOperator3.getQueue() ).getThreadingPreference(), equalTo( SINGLE_THREADED ) );
        assertTrue( pipelineOperator3.getDrainerPool() instanceof NonBlockingTupleQueueDrainerPool );
        assertThat( newPipelineReplicas[ 2 ].id().pipelineId, equalTo( new PipelineId( REGION_ID, 4 ) ) );
        assertTrue( newPipelineReplicas[ 2 ].getSelfPipelineTupleQueue() instanceof DefaultOperatorTupleQueue );
        assertTrue( pipelineOperator4.getQueue() instanceof PartitionedOperatorTupleQueue );
        assertTrue( pipelineOperator4.getDrainerPool() instanceof NonBlockingTupleQueueDrainerPool );

        final PipelineReplicaMeter meter0 = newPipelineReplicas[ 0 ].getMeter();
        assertThat( meter0.getHeadOperatorId(), equalTo( pipelineOperator0.getOperatorDef().getId() ) );
        assertThat( meter0.getConsumedPortCount(), equalTo( pipelineOperator0.getOperatorDef().getInputPortCount() ) );
        assertThat( meter0.getTailOperatorId(), equalTo( pipelineOperator1.getOperatorDef().getId() ) );
        assertThat( meter0.getProducedPortCount(), equalTo( pipelineOperator1.getOperatorDef().getOutputPortCount() ) );

        final PipelineReplicaMeter meter1 = newPipelineReplicas[ 1 ].getMeter();
        assertThat( meter1.getHeadOperatorId(), equalTo( pipelineOperator2.getOperatorDef().getId() ) );
        assertThat( meter1.getConsumedPortCount(), equalTo( pipelineOperator2.getOperatorDef().getInputPortCount() ) );
        assertThat( meter1.getTailOperatorId(), equalTo( pipelineOperator3.getOperatorDef().getId() ) );
        assertThat( meter1.getProducedPortCount(), equalTo( pipelineOperator3.getOperatorDef().getOutputPortCount() ) );

        final PipelineReplicaMeter meter2 = newPipelineReplicas[ 2 ].getMeter();
        assertThat( meter2.getHeadOperatorId(), equalTo( pipelineOperator4.getOperatorDef().getId() ) );
        assertThat( meter2.getConsumedPortCount(), equalTo( pipelineOperator4.getOperatorDef().getInputPortCount() ) );
        assertThat( meter2.getTailOperatorId(), equalTo( pipelineOperator4.getOperatorDef().getId() ) );
        assertThat( meter2.getProducedPortCount(), equalTo( pipelineOperator4.getOperatorDef().getOutputPortCount() ) );

        for ( PipelineReplica p : newPipelineReplicas )
        {
            assertPipelineReplicaMeter( p );
        }
    }

    @Test
    public void shouldSplitPipelineIntoSingleOperatorPipelines ()
    {
        final OperatorDef operatorDef0 = OperatorDefBuilder.newInstance( "op0", PartitionedStatefulInput1Output1Operator.class )
                                                           .setPartitionFieldNames( singletonList( "field" ) )
                                                           .build();
        final OperatorDef operatorDef1 = OperatorDefBuilder.newInstance( "op1", StatelessInput1Output1Operator.class ).build();
        final OperatorDef operatorDef2 = OperatorDefBuilder.newInstance( "op2", StatelessInput1Output1Operator.class ).build();
        final OperatorDef operatorDef3 = OperatorDefBuilder.newInstance( "op3", StatelessInput1Output1Operator.class ).build();
        final OperatorDef operatorDef4 = OperatorDefBuilder.newInstance( "op4", StatelessInput1Output1Operator.class ).build();
        final OperatorDef operatorDef5 = OperatorDefBuilder.newInstance( "op5", StatelessInput1Output1Operator.class ).build();

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

        final RegionDef regionDef = new RegionDef( REGION_ID, STATELESS, emptyList(),
                                                   asList( operatorDef1, operatorDef2, operatorDef3, operatorDef4, operatorDef5 ) );

        final RegionExecutionPlan regionExecutionPlan = new RegionExecutionPlan( regionDef, singletonList( 0 ), 1 );
        final Region region = regionManager.createRegion( flow, regionExecutionPlan );
        initialize( region );

        final Region newRegion = pipelineTransformer.splitPipeline( region, asList( 0, 1, 2, 3, 4 ) );

        final PipelineReplica[] newPipelineReplicas = newRegion.getReplicaPipelines( 0 );
        final OperatorReplica pipelineOperator0 = newPipelineReplicas[ 0 ].getOperator( 0 );
        final OperatorReplica pipelineOperator1 = newPipelineReplicas[ 1 ].getOperator( 0 );
        final OperatorReplica pipelineOperator2 = newPipelineReplicas[ 2 ].getOperator( 0 );
        final OperatorReplica pipelineOperator3 = newPipelineReplicas[ 3 ].getOperator( 0 );
        final OperatorReplica pipelineOperator4 = newPipelineReplicas[ 4 ].getOperator( 0 );

        assertThat( newPipelineReplicas[ 0 ].id().pipelineId, equalTo( new PipelineId( REGION_ID, 0 ) ) );
        assertTrue( newPipelineReplicas[ 0 ].getSelfPipelineTupleQueue() instanceof EmptyOperatorTupleQueue );
        assertThat( ( (DefaultOperatorTupleQueue) pipelineOperator0.getQueue() ).getThreadingPreference(), equalTo( MULTI_THREADED ) );
        assertTrue( pipelineOperator0.getDrainerPool() instanceof BlockingTupleQueueDrainerPool );
        assertThat( pipelineOperator0.getOutputSupplier().getClass(),
                    equalTo( config.getRegionManagerConfig().getPipelineTailOperatorOutputSupplierClass() ) );
        assertThat( newPipelineReplicas[ 1 ].id().pipelineId, equalTo( new PipelineId( REGION_ID, 1 ) ) );
        assertTrue( newPipelineReplicas[ 1 ].getSelfPipelineTupleQueue() instanceof EmptyOperatorTupleQueue );
        assertThat( ( (DefaultOperatorTupleQueue) pipelineOperator1.getQueue() ).getThreadingPreference(), equalTo( MULTI_THREADED ) );
        assertTrue( pipelineOperator1.getDrainerPool() instanceof BlockingTupleQueueDrainerPool );
        assertThat( pipelineOperator1.getOutputSupplier().getClass(),
                    equalTo( config.getRegionManagerConfig().getPipelineTailOperatorOutputSupplierClass() ) );
        assertThat( newPipelineReplicas[ 2 ].id().pipelineId, equalTo( new PipelineId( REGION_ID, 2 ) ) );
        assertTrue( newPipelineReplicas[ 2 ].getSelfPipelineTupleQueue() instanceof EmptyOperatorTupleQueue );
        assertThat( ( (DefaultOperatorTupleQueue) pipelineOperator2.getQueue() ).getThreadingPreference(), equalTo( MULTI_THREADED ) );
        assertTrue( pipelineOperator2.getDrainerPool() instanceof BlockingTupleQueueDrainerPool );
        assertThat( pipelineOperator2.getOutputSupplier().getClass(),
                    equalTo( config.getRegionManagerConfig().getPipelineTailOperatorOutputSupplierClass() ) );
        assertThat( newPipelineReplicas[ 3 ].id().pipelineId, equalTo( new PipelineId( REGION_ID, 3 ) ) );
        assertTrue( newPipelineReplicas[ 3 ].getSelfPipelineTupleQueue() instanceof EmptyOperatorTupleQueue );
        assertThat( ( (DefaultOperatorTupleQueue) pipelineOperator3.getQueue() ).getThreadingPreference(), equalTo( MULTI_THREADED ) );
        assertTrue( pipelineOperator3.getDrainerPool() instanceof BlockingTupleQueueDrainerPool );
        assertThat( pipelineOperator3.getOutputSupplier().getClass(),
                    equalTo( config.getRegionManagerConfig().getPipelineTailOperatorOutputSupplierClass() ) );
        assertThat( newPipelineReplicas[ 4 ].id().pipelineId, equalTo( new PipelineId( REGION_ID, 4 ) ) );
        assertTrue( newPipelineReplicas[ 4 ].getSelfPipelineTupleQueue() instanceof EmptyOperatorTupleQueue );
        assertThat( ( (DefaultOperatorTupleQueue) pipelineOperator4.getQueue() ).getThreadingPreference(), equalTo( MULTI_THREADED ) );
        assertTrue( pipelineOperator4.getDrainerPool() instanceof BlockingTupleQueueDrainerPool );
        assertThat( pipelineOperator4.getOutputSupplier().getClass(),
                    equalTo( config.getRegionManagerConfig().getPipelineTailOperatorOutputSupplierClass() ) );

        for ( PipelineReplica newPipelineReplica : newPipelineReplicas )
        {
            final PipelineReplicaMeter meter = newPipelineReplica.getMeter();
            final OperatorReplica operator = newPipelineReplica.getOperator( 0 );
            assertThat( meter.getHeadOperatorId(), equalTo( operator.getOperatorDef().getId() ) );
            assertThat( meter.getConsumedPortCount(), equalTo( operator.getOperatorDef().getInputPortCount() ) );
            assertThat( meter.getTailOperatorId(), equalTo( operator.getOperatorDef().getId() ) );
            assertThat( meter.getProducedPortCount(), equalTo( operator.getOperatorDef().getOutputPortCount() ) );
            assertPipelineReplicaMeter( newPipelineReplica );
        }
    }

    @Test
    public void shouldCopyNonSplitPipelinesAtHeadOfTheRegion ()
    {
        final OperatorDef operatorDef0 = OperatorDefBuilder.newInstance( "op0", PartitionedStatefulInput1Output1Operator.class )
                                                           .setPartitionFieldNames( singletonList( "field" ) )
                                                           .build();
        final OperatorDef operatorDef1 = OperatorDefBuilder.newInstance( "op1", StatelessInput1Output1Operator.class ).build();
        final OperatorDef operatorDef2 = OperatorDefBuilder.newInstance( "op2", StatelessInput1Output1Operator.class ).build();
        final OperatorDef operatorDef3 = OperatorDefBuilder.newInstance( "op3", StatelessInput1Output1Operator.class ).build();
        final OperatorDef operatorDef4 = OperatorDefBuilder.newInstance( "op4", StatelessInput1Output1Operator.class ).build();
        final OperatorDef operatorDef5 = OperatorDefBuilder.newInstance( "op5", StatelessInput1Output1Operator.class ).build();

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
                                                   asList( operatorDef0,
                                                           operatorDef1,
                                                           operatorDef2,
                                                           operatorDef3,
                                                           operatorDef4,
                                                           operatorDef5 ) );

        final RegionExecutionPlan regionExecutionPlan = new RegionExecutionPlan( regionDef, asList( 0, 1, 2 ), 2 );
        final Region region = regionManager.createRegion( flow, regionExecutionPlan );
        initialize( region );

        final Region newRegion = pipelineTransformer.splitPipeline( region, asList( 2, 4 ) );

        assertThat( newRegion.getPipelineReplicas( 0 ), equalTo( region.getPipelineReplicas( 0 ) ) );
        assertThat( newRegion.getPipelineReplicas( 1 ), equalTo( region.getPipelineReplicas( 1 ) ) );
    }

    @Test
    public void shouldCopyNonSplitPipelinesAtTailOfTheRegion ()
    {
        final OperatorDef operatorDef0 = OperatorDefBuilder.newInstance( "op0", PartitionedStatefulInput1Output1Operator.class )
                                                           .setPartitionFieldNames( singletonList( "field" ) )
                                                           .build();
        final OperatorDef operatorDef1 = OperatorDefBuilder.newInstance( "op1", StatelessInput1Output1Operator.class ).build();
        final OperatorDef operatorDef2 = OperatorDefBuilder.newInstance( "op2", StatelessInput1Output1Operator.class ).build();
        final OperatorDef operatorDef3 = OperatorDefBuilder.newInstance( "op3", StatelessInput1Output1Operator.class ).build();
        final OperatorDef operatorDef4 = OperatorDefBuilder.newInstance( "op4", StatelessInput1Output1Operator.class ).build();
        final OperatorDef operatorDef5 = OperatorDefBuilder.newInstance( "op5", StatelessInput1Output1Operator.class ).build();

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
                                                   asList( operatorDef0,
                                                           operatorDef1,
                                                           operatorDef2,
                                                           operatorDef3,
                                                           operatorDef4,
                                                           operatorDef5 ) );

        final RegionExecutionPlan regionExecutionPlan = new RegionExecutionPlan( regionDef, asList( 0, 3, 4 ), 2 );
        final Region region = regionManager.createRegion( flow, regionExecutionPlan );
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

        final RegionExecutionPlan regionExecutionPlan = new RegionExecutionPlan( regionDef, asList( 0, 2, 4 ), 2 );

        assertFalse( pipelineTransformer.checkPipelineStartIndicesToSplit( regionExecutionPlan, singletonList( 0 ) ) );
        assertFalse( pipelineTransformer.checkPipelineStartIndicesToSplit( regionExecutionPlan, singletonList( -1 ) ) );
        assertFalse( pipelineTransformer.checkPipelineStartIndicesToSplit( regionExecutionPlan, asList( 0, 1, 2 ) ) );
        assertFalse( pipelineTransformer.checkPipelineStartIndicesToSplit( regionExecutionPlan, asList( 4, 7 ) ) );

        assertTrue( pipelineTransformer.checkPipelineStartIndicesToSplit( regionExecutionPlan, asList( 0, 1 ) ) );
        assertTrue( pipelineTransformer.checkPipelineStartIndicesToSplit( regionExecutionPlan, asList( 4, 5 ) ) );
        assertTrue( pipelineTransformer.checkPipelineStartIndicesToSplit( regionExecutionPlan, asList( 4, 5, 6 ) ) );
    }

    private void initialize ( final Region region )
    {
        for ( int replicaIndex = 0; replicaIndex < region.getExecutionPlan().getReplicaCount(); replicaIndex++ )
        {
            final PipelineReplica[] pipelineReplicas = region.getReplicaPipelines( replicaIndex );
            final int i = pipelineReplicas[ 0 ].getOperator( 0 ).getOperatorDef().getInputPortCount();
            final UpstreamConnectionStatus[] statuses = new UpstreamConnectionStatus[ i ];
            fill( statuses, UpstreamConnectionStatus.ACTIVE );
            UpstreamContext uc = new UpstreamContext( 0, statuses );
            for ( PipelineReplica pipelineReplica : region.getReplicaPipelines( replicaIndex ) )
            {
                pipelineReplica.init( uc );
                uc = pipelineReplica.getOperator( pipelineReplica.getOperatorCount() - 1 ).getSelfUpstreamContext();
            }
        }
    }

    private Tuple newTuple ( final String key, final Object value )
    {
        final Tuple tuple = new Tuple();
        tuple.set( key, value );
        return tuple;
    }

    private List<Tuple> drainDefaultPortGreedily ( final OperatorReplica operatorReplica )
    {
        final GreedyDrainer drainer = new GreedyDrainer( operatorReplica.getOperatorDef().getInputPortCount() );
        operatorReplica.getQueue().drain( drainer );
        return drainer.getResult().getTuples( 0 );
    }

    @OperatorSpec( type = STATELESS, inputPortCount = 0, outputPortCount = 1 )
    @OperatorSchema( outputs = { @PortSchema( portIndex = 0, scope = EXACT_FIELD_SET, fields = { @SchemaField( name = "field", type = Integer.class ) } ) } )
    public static class StatelessInput0Output1Operator extends NopOperator
    {

        @Override
        public SchedulingStrategy init ( final InitializationContext context )
        {
            return ScheduleWhenAvailable.INSTANCE;
        }

    }


    @OperatorSpec( type = STATELESS, inputPortCount = 1, outputPortCount = 1 )
    @OperatorSchema( inputs = { @PortSchema( portIndex = 0, scope = EXACT_FIELD_SET, fields = { @SchemaField( name = "field", type = Integer.class ) } ) }, outputs = { @PortSchema( portIndex = 0, scope = EXACT_FIELD_SET, fields = { @SchemaField( name = "field", type = Integer.class ) } ) } )
    public static class StatelessInput1Output1Operator extends NopOperator
    {

        @Override
        public SchedulingStrategy init ( final InitializationContext context )
        {
            return scheduleWhenTuplesAvailableOnDefaultPort( 1 );
        }

    }


    @OperatorSpec( type = OperatorType.PARTITIONED_STATEFUL, inputPortCount = 1, outputPortCount = 1 )
    @OperatorSchema( inputs = { @PortSchema( portIndex = 0, scope = EXACT_FIELD_SET, fields = { @SchemaField( name = "field", type = Integer.class ) } ) }, outputs = { @PortSchema( portIndex = 0, scope = EXACT_FIELD_SET, fields = { @SchemaField( name = "field", type = Integer.class ) } ) } )
    public static class PartitionedStatefulInput1Output1Operator extends NopOperator
    {

        @Override
        public SchedulingStrategy init ( final InitializationContext context )
        {
            return scheduleWhenTuplesAvailableOnDefaultPort( 1 );
        }

    }


    @OperatorSpec( type = STATEFUL, inputPortCount = 1, outputPortCount = 1 )
    @OperatorSchema( inputs = { @PortSchema( portIndex = 0, scope = EXACT_FIELD_SET, fields = { @SchemaField( name = "field", type = Integer.class ) } ) }, outputs = { @PortSchema( portIndex = 0, scope = EXACT_FIELD_SET, fields = { @SchemaField( name = "field", type = Integer.class ) } ) } )
    public static class StatefulInput1Output1Operator extends NopOperator
    {

        @Override
        public SchedulingStrategy init ( final InitializationContext context )
        {
            return scheduleWhenTuplesAvailableOnDefaultPort( 1 );
        }

    }


    public static abstract class NopOperator implements Operator
    {

        @Override
        public void invoke ( final InvocationContext invocationContext )
        {

        }

    }

}
