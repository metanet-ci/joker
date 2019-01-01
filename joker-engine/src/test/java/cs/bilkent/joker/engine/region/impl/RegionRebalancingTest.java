package cs.bilkent.joker.engine.region.impl;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import cs.bilkent.joker.engine.config.JokerConfig;
import static cs.bilkent.joker.engine.config.ThreadingPref.MULTI_THREADED;
import cs.bilkent.joker.engine.flow.RegionDef;
import cs.bilkent.joker.engine.flow.RegionExecPlan;
import cs.bilkent.joker.engine.kvstore.OperatorKVStore;
import cs.bilkent.joker.engine.kvstore.impl.OperatorKVStoreManagerImpl;
import cs.bilkent.joker.engine.partition.PartitionDistribution;
import cs.bilkent.joker.engine.partition.PartitionKeyExtractor;
import cs.bilkent.joker.engine.partition.PartitionKeyExtractorFactory;
import cs.bilkent.joker.engine.partition.PartitionService;
import static cs.bilkent.joker.engine.partition.PartitionUtil.getPartitionId;
import cs.bilkent.joker.engine.partition.impl.PartitionKeyExtractor1;
import cs.bilkent.joker.engine.partition.impl.PartitionKeyExtractorFactoryImpl;
import cs.bilkent.joker.engine.partition.impl.PartitionServiceImpl;
import cs.bilkent.joker.engine.pipeline.OperatorReplica;
import cs.bilkent.joker.engine.pipeline.PipelineReplica;
import cs.bilkent.joker.engine.pipeline.PipelineReplicaId;
import cs.bilkent.joker.engine.pipeline.impl.invocation.FusedInvocationCtx;
import cs.bilkent.joker.engine.pipeline.impl.invocation.FusedPartitionedInvocationCtx;
import cs.bilkent.joker.engine.region.PipelineTransformer;
import cs.bilkent.joker.engine.region.Region;
import cs.bilkent.joker.engine.region.impl.RegionManagerImplTest.FlowExample6;
import static cs.bilkent.joker.engine.region.impl.RegionManagerImplTest.FlowExample6.PARTITION_KEY_FIELD;
import static cs.bilkent.joker.engine.region.impl.RegionManagerImplTest.assertBlockingTupleQueueDrainerPool;
import static cs.bilkent.joker.engine.region.impl.RegionManagerImplTest.assertDefaultOperatorQueue;
import static cs.bilkent.joker.engine.region.impl.RegionManagerImplTest.assertEmptyPipelineQueue;
import static cs.bilkent.joker.engine.region.impl.RegionManagerImplTest.assertNonBlockingTupleQueueDrainerPool;
import static cs.bilkent.joker.engine.region.impl.RegionManagerImplTest.assertOperatorDef;
import static cs.bilkent.joker.engine.region.impl.RegionManagerImplTest.assertPartitionedOperatorQueue;
import cs.bilkent.joker.engine.tuplequeue.OperatorQueue;
import cs.bilkent.joker.engine.tuplequeue.TupleQueueDrainer;
import cs.bilkent.joker.engine.tuplequeue.impl.OperatorQueueManagerImpl;
import static cs.bilkent.joker.engine.tuplequeue.impl.drainer.NonBlockingMultiPortDisjunctiveDrainer.newGreedyDrainer;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.impl.TuplesImpl;
import cs.bilkent.joker.test.AbstractJokerTest;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith( value = Parameterized.class )
public class RegionRebalancingTest extends AbstractJokerTest
{

    @Parameters( name = "initialReplicaCount={0}, rebalancedReplicaCount={1}" )
    public static Collection<Object[]> data ()
    {
        return asList( new Object[][] { { 2, 4 }, { 4, 2 } } );
    }

    private static final PartitionKeyExtractor EXTRACTOR = new PartitionKeyExtractor1( singletonList( PARTITION_KEY_FIELD ) );


    private final JokerConfig config = new JokerConfig();

    private final IdGenerator idGenerator = new IdGenerator();

    private final RegionDefFormerImpl regionDefFormer = new RegionDefFormerImpl( idGenerator );

    private final PartitionService partitionService = new PartitionServiceImpl( config );

    private final PartitionKeyExtractorFactory partitionKeyExtractorFactory = new PartitionKeyExtractorFactoryImpl();

    private final OperatorQueueManagerImpl operatorQueueManager = new OperatorQueueManagerImpl( config, partitionKeyExtractorFactory );

    private final OperatorKVStoreManagerImpl operatorKVStoreManager = new OperatorKVStoreManagerImpl();

    private final PipelineTransformer pipelineTransformer = new PipelineTransformerImpl( config,
                                                                                         partitionService,
                                                                                         operatorQueueManager,
                                                                                         operatorKVStoreManager,
                                                                                         partitionKeyExtractorFactory );

    private final RegionManagerImpl regionManager = new RegionManagerImpl( config,
                                                                           partitionService,
                                                                           operatorKVStoreManager,
                                                                           operatorQueueManager,
                                                                           pipelineTransformer,
                                                                           partitionKeyExtractorFactory );


    private final int initialReplicaCount;

    private final int rebalancedReplicaCount;

    private final Set<Object> keys = new HashSet<>();

    public RegionRebalancingTest ( final int initialReplicaCount, final int rebalancedReplicaCount )
    {
        this.initialReplicaCount = initialReplicaCount;
        this.rebalancedReplicaCount = rebalancedReplicaCount;
    }

    @Test
    public void shouldRebalancePartitionedStatefulRegion1 ()
    {
        final FlowExample6 flowExample6 = new FlowExample6();

        final List<RegionDef> regionDefs = regionDefFormer.createRegions( flowExample6.flow );
        final RegionDef regionDef = regionDefs.get( 1 );
        final RegionExecPlan regionExecPlan = new RegionExecPlan( regionDef, asList( 0, 1 ), initialReplicaCount );
        final Region region = regionManager.createRegion( flowExample6.flow, regionExecPlan );

        final PipelineReplica[] pipelineReplicas0 = region.getPipelineReplicas( 0 );
        final PipelineReplica[] pipelineReplicas1 = region.getPipelineReplicas( 1 );
        final PartitionDistribution partitionDistribution = partitionService.getPartitionDistributionOrFail( regionDef.getRegionId() );
        for ( int partitionId = 0; partitionId < config.getPartitionServiceConfig().getPartitionCount(); partitionId++ )
        {
            final Tuple tuple = generateTuple( partitionId );
            final int replicaIndex = partitionDistribution.getReplicaIndex( partitionId );
            pipelineReplicas0[ replicaIndex ].getOperatorReplica( 0 ).getQueue().offer( 0, singletonList( tuple ) );
            pipelineReplicas1[ replicaIndex ].getQueue().offer( 0, singletonList( tuple ) );
        }

        final Region rebalancedRegion = regionManager.rebalanceRegion( flowExample6.flow, regionDef.getRegionId(), rebalancedReplicaCount );
        verifyRebalancedStatefulRegion1( flowExample6, regionDef, rebalancedRegion );
        if ( rebalancedReplicaCount < initialReplicaCount )
        {
            for ( int replicaIndex = initialReplicaCount; replicaIndex < rebalancedReplicaCount; replicaIndex++ )
            {
                assertNull( operatorQueueManager.getDefaultQueue( region.getRegionId(), flowExample6.operatorDef1.getId(), replicaIndex ) );
            }
        }

        final OperatorQueue[] partitionedStatefulOperatorQueues = operatorQueueManager.getPartitionedQueues( region.getRegionId(),
                                                                                                             flowExample6.operatorDef2.getId() );
        assertNotNull( partitionedStatefulOperatorQueues );
        assertEquals( rebalancedReplicaCount, partitionedStatefulOperatorQueues.length );
    }

    private void verifyRebalancedStatefulRegion1 ( final FlowExample6 flowExample6, final RegionDef regionDef, final Region region )
    {
        assertNotNull( region );

        for ( int replicaIndex = 0; replicaIndex < region.getExecPlan().getReplicaCount(); replicaIndex++ )
        {
            final PipelineReplica[] pipelines = region.getReplicaPipelines( replicaIndex );
            assertEquals( 2, pipelines.length );
            final PipelineReplica pipeline0 = pipelines[ 0 ];
            final PipelineReplica pipeline1 = pipelines[ 1 ];
            assertDefaultOperatorQueue( pipeline0, flowExample6.operatorDef1.getInputPortCount() );
            assertEmptyPipelineQueue( pipeline0 );

            assertEquals( new PipelineReplicaId( regionDef.getRegionId(), 0, replicaIndex ), pipeline0.id() );
            assertEquals( new PipelineReplicaId( regionDef.getRegionId(), 1, replicaIndex ), pipeline1.id() );
            assertEquals( 1, pipeline0.getOperatorCount() );
            assertEquals( 2, pipeline1.getOperatorCount() );

            final OperatorReplica operatorReplica00 = pipeline0.getOperatorReplica( 0 );
            assertOperatorDef( operatorReplica00, 0, flowExample6.operatorDef1 );
            RegionManagerImplTest.assertDefaultOperatorQueue( operatorReplica00,
                                                              flowExample6.operatorDef1.getInputPortCount(),
                                                              MULTI_THREADED );
            assertNotNull( operatorQueueManager.getDefaultQueue( region.getRegionId(), flowExample6.operatorDef1.getId(), replicaIndex ) );
            assertBlockingTupleQueueDrainerPool( operatorReplica00 );
            assertNull( operatorKVStoreManager.getDefaultKVStore( region.getRegionId(), flowExample6.operatorDef1.getId() ) );
            assertNull( operatorKVStoreManager.getPartitionedKVStores( region.getRegionId(), flowExample6.operatorDef1.getId() ) );

            final OperatorReplica operatorReplica10 = pipeline1.getOperatorReplica( 0 );
            assertOperatorDef( operatorReplica10, 0, flowExample6.operatorDef2 );
            assertPartitionedOperatorQueue( operatorReplica10 );
            assertNonBlockingTupleQueueDrainerPool( operatorReplica10 );

            assertTrue( operatorReplica10.getInvocationCtx( 1 ) instanceof FusedInvocationCtx );
        }

        assertNull( operatorKVStoreManager.getDefaultKVStore( region.getRegionId(), flowExample6.operatorDef3.getId() ) );
        assertNull( operatorKVStoreManager.getPartitionedKVStores( region.getRegionId(), flowExample6.operatorDef3.getId() ) );

        final OperatorKVStore[] partitionedOperatorKVStores = operatorKVStoreManager.getPartitionedKVStores( region.getRegionId(),
                                                                                                             flowExample6.operatorDef2.getId() );
        assertNotNull( partitionedOperatorKVStores );
        assertEquals( rebalancedReplicaCount, partitionedOperatorKVStores.length );

        final PipelineReplica[] pipelineReplicas0 = region.getPipelineReplicas( 0 );
        final PipelineReplica[] pipelineReplicas1 = region.getPipelineReplicas( 1 );
        for ( PipelineReplica pipelineReplica : pipelineReplicas1 )
        {
            final OperatorReplica operator = pipelineReplica.getOperatorReplica( 0 );
            final OperatorDef operatorDef = operator.getOperatorDef( 0 );
            final TuplesImpl result = new TuplesImpl( operatorDef.getInputPortCount() );
            final TupleQueueDrainer drainer = newGreedyDrainer( operatorDef.getId(), operatorDef.getInputPortCount(), Integer.MAX_VALUE );
            pipelineReplica.getQueue().drain( drainer, key -> result );
            assertTrue( result.isEmpty() );
        }

        final Set<Object> keys1 = new HashSet<>( keys );
        final Set<Object> keys2 = new HashSet<>( keys );

        final PartitionDistribution partitionDistribution = partitionService.getPartitionDistributionOrFail( region.getRegionId() );
        for ( int partitionId = 0; partitionId < partitionService.getPartitionCount(); partitionId++ )
        {
            final int replicaIndex = partitionDistribution.getReplicaIndex( partitionId );
            drainOperatorQueue( keys1, pipelineReplicas0[ replicaIndex ].getOperatorReplica( 0 ) );
            drainOperatorQueue( keys2, pipelineReplicas1[ replicaIndex ].getOperatorReplica( 0 ) );
        }

        assertTrue( keys1.isEmpty() );
        assertTrue( keys2.isEmpty() );
    }

    @Test
    public void shouldRebalancePartitionedStatefulRegion2 ()
    {
        final FlowExample6 flowExample6 = new FlowExample6();

        final List<RegionDef> regionDefs = regionDefFormer.createRegions( flowExample6.flow );
        final RegionDef regionDef = regionDefs.get( 1 );
        final RegionExecPlan regionExecPlan = new RegionExecPlan( regionDef, asList( 0, 2 ), initialReplicaCount );
        final Region region = regionManager.createRegion( flowExample6.flow, regionExecPlan );

        final PipelineReplica[] pipelineReplicas0 = region.getPipelineReplicas( 0 );
        final PipelineReplica[] pipelineReplicas1 = region.getPipelineReplicas( 1 );
        final PartitionDistribution partitionDistribution = partitionService.getPartitionDistributionOrFail( regionDef.getRegionId() );
        for ( int partitionId = 0; partitionId < config.getPartitionServiceConfig().getPartitionCount(); partitionId++ )
        {
            final Tuple tuple = generateTuple( partitionId );
            final int replicaIndex = partitionDistribution.getReplicaIndex( partitionId );
            pipelineReplicas0[ replicaIndex ].getOperatorReplica( 0 ).getQueue().offer( 0, singletonList( tuple ) );
            pipelineReplicas1[ replicaIndex ].getOperatorReplica( 0 ).getQueue().offer( 0, singletonList( tuple ) );
        }

        final Region rebalancedRegion = regionManager.rebalanceRegion( flowExample6.flow, regionDef.getRegionId(), rebalancedReplicaCount );
        verifyRebalancedStatefulRegion2( flowExample6, regionDef, rebalancedRegion );
        if ( rebalancedReplicaCount < initialReplicaCount )
        {
            for ( int replicaIndex = initialReplicaCount; replicaIndex < rebalancedReplicaCount; replicaIndex++ )
            {
                assertNull( operatorQueueManager.getDefaultQueue( region.getRegionId(), flowExample6.operatorDef1.getId(), replicaIndex ) );
                assertNull( operatorQueueManager.getDefaultQueue( region.getRegionId(), flowExample6.operatorDef3.getId(), replicaIndex ) );
            }
        }

        assertNull( operatorQueueManager.getPartitionedQueues( region.getRegionId(), flowExample6.operatorDef2.getId() ) );
    }

    private void verifyRebalancedStatefulRegion2 ( final FlowExample6 flowExample6, final RegionDef regionDef, final Region region )
    {
        assertNotNull( region );

        for ( int replicaIndex = 0; replicaIndex < region.getExecPlan().getReplicaCount(); replicaIndex++ )
        {
            final PipelineReplica[] pipelines = region.getReplicaPipelines( replicaIndex );
            assertEquals( 2, pipelines.length );
            final PipelineReplica pipeline0 = pipelines[ 0 ];
            final PipelineReplica pipeline1 = pipelines[ 1 ];
            assertDefaultOperatorQueue( pipeline0, flowExample6.operatorDef1.getInputPortCount() );
            assertEmptyPipelineQueue( pipeline0 );

            assertEquals( new PipelineReplicaId( regionDef.getRegionId(), 0, replicaIndex ), pipeline0.id() );
            assertEquals( new PipelineReplicaId( regionDef.getRegionId(), 2, replicaIndex ), pipeline1.id() );
            assertEquals( 2, pipeline0.getOperatorCount() );
            assertEquals( 1, pipeline1.getOperatorCount() );

            final OperatorReplica operatorReplica00 = pipeline0.getOperatorReplica( 0 );
            assertOperatorDef( operatorReplica00, 0, flowExample6.operatorDef1 );
            RegionManagerImplTest.assertDefaultOperatorQueue( operatorReplica00,
                                                              flowExample6.operatorDef1.getInputPortCount(),
                                                              MULTI_THREADED );
            assertNotNull( operatorQueueManager.getDefaultQueue( region.getRegionId(), flowExample6.operatorDef1.getId(), replicaIndex ) );
            assertBlockingTupleQueueDrainerPool( operatorReplica00 );
            assertNull( operatorKVStoreManager.getDefaultKVStore( region.getRegionId(), flowExample6.operatorDef1.getId() ) );
            assertNull( operatorKVStoreManager.getPartitionedKVStores( region.getRegionId(), flowExample6.operatorDef1.getId() ) );

            assertTrue( operatorReplica00.getInvocationCtx( 1 ) instanceof FusedPartitionedInvocationCtx );

            final OperatorReplica operatorReplica10 = pipeline1.getOperatorReplica( 0 );
            assertOperatorDef( operatorReplica10, 0, flowExample6.operatorDef3 );
            RegionManagerImplTest.assertDefaultOperatorQueue( operatorReplica10,
                                                              flowExample6.operatorDef3.getInputPortCount(),
                                                              MULTI_THREADED );
            assertBlockingTupleQueueDrainerPool( operatorReplica10 );
        }

        assertNull( operatorKVStoreManager.getDefaultKVStore( region.getRegionId(), flowExample6.operatorDef3.getId() ) );
        assertNull( operatorKVStoreManager.getPartitionedKVStores( region.getRegionId(), flowExample6.operatorDef3.getId() ) );

        final OperatorKVStore[] partitionedOperatorKVStores = operatorKVStoreManager.getPartitionedKVStores( region.getRegionId(),
                                                                                                             flowExample6.operatorDef2.getId() );
        assertNotNull( partitionedOperatorKVStores );
        assertEquals( rebalancedReplicaCount, partitionedOperatorKVStores.length );

        final PipelineReplica[] pipelineReplicas0 = region.getPipelineReplicas( 0 );
        final PipelineReplica[] pipelineReplicas1 = region.getPipelineReplicas( 1 );
        for ( PipelineReplica pipelineReplica : pipelineReplicas1 )
        {
            final OperatorReplica operator = pipelineReplica.getOperatorReplica( 0 );
            final OperatorDef operatorDef = operator.getOperatorDef( 0 );
            final TuplesImpl result = new TuplesImpl( operatorDef.getInputPortCount() );
            final TupleQueueDrainer drainer = newGreedyDrainer( operatorDef.getId(), operatorDef.getInputPortCount(), Integer.MAX_VALUE );
            pipelineReplica.getQueue().drain( drainer, key -> result );
            assertTrue( result.isEmpty() );
        }

        final Set<Object> keys1 = new HashSet<>( keys );
        final Set<Object> keys2 = new HashSet<>( keys );

        final PartitionDistribution partitionDistribution = partitionService.getPartitionDistributionOrFail( region.getRegionId() );
        for ( int partitionId = 0; partitionId < partitionService.getPartitionCount(); partitionId++ )
        {
            final int replicaIndex = partitionDistribution.getReplicaIndex( partitionId );
            drainOperatorQueue( keys1, pipelineReplicas0[ replicaIndex ].getOperatorReplica( 0 ) );
            drainOperatorQueue( keys2, pipelineReplicas1[ replicaIndex ].getOperatorReplica( 0 ) );
        }

        assertTrue( keys1.isEmpty() );
        assertTrue( keys2.isEmpty() );
    }

    private void drainOperatorQueue ( final Set<Object> keys1, final OperatorReplica operator )
    {
        final OperatorQueue operatorQueue = operator.getQueue();
        final OperatorDef operatorDef = operator.getOperatorDef( 0 );
        final TuplesImpl result = new TuplesImpl( operatorDef.getInputPortCount() );
        final TupleQueueDrainer drainer = newGreedyDrainer( operatorDef.getId(), operatorDef.getInputPortCount(), Integer.MAX_VALUE );
        operatorQueue.drain( drainer, key -> result );
        if ( result.isNonEmpty() )
        {
            for ( Tuple tuple : result.getTuplesByDefaultPort() )
            {
                keys1.remove( tuple.get( PARTITION_KEY_FIELD ) );
            }
        }
    }

    private Tuple generateTuple ( final int partitionId )
    {
        final Tuple tuple = new Tuple();
        int i = 0;
        while ( true )
        {
            if ( keys.contains( i ) )
            {
                i++;
                continue;
            }

            tuple.set( PARTITION_KEY_FIELD, i );
            final int partitionHash = EXTRACTOR.getHash( tuple );

            if ( getPartitionId( partitionHash, config.getPartitionServiceConfig().getPartitionCount() ) == partitionId )
            {
                keys.add( i );
                return tuple;
            }

            i++;
        }
    }

}
