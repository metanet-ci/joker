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
import static cs.bilkent.joker.engine.config.ThreadingPreference.MULTI_THREADED;
import static cs.bilkent.joker.engine.config.ThreadingPreference.SINGLE_THREADED;
import cs.bilkent.joker.engine.kvstore.impl.OperatorKVStoreManagerImpl;
import cs.bilkent.joker.engine.partition.PartitionDistribution;
import cs.bilkent.joker.engine.partition.PartitionKeyExtractor;
import cs.bilkent.joker.engine.partition.PartitionService;
import static cs.bilkent.joker.engine.partition.PartitionUtil.getPartitionId;
import cs.bilkent.joker.engine.partition.impl.PartitionKeyExtractor1;
import cs.bilkent.joker.engine.partition.impl.PartitionKeyExtractorFactoryImpl;
import cs.bilkent.joker.engine.partition.impl.PartitionServiceImpl;
import cs.bilkent.joker.engine.pipeline.OperatorReplica;
import cs.bilkent.joker.engine.pipeline.PipelineId;
import cs.bilkent.joker.engine.pipeline.PipelineReplica;
import cs.bilkent.joker.engine.pipeline.PipelineReplicaId;
import cs.bilkent.joker.engine.region.PipelineTransformer;
import cs.bilkent.joker.engine.region.Region;
import cs.bilkent.joker.engine.region.RegionConfig;
import cs.bilkent.joker.engine.region.RegionDef;
import cs.bilkent.joker.engine.region.impl.RegionManagerImplTest.FlowExample2;
import cs.bilkent.joker.engine.region.impl.RegionManagerImplTest.FlowExample6;
import static cs.bilkent.joker.engine.region.impl.RegionManagerImplTest.FlowExample6.PARTITION_KEY_FIELD;
import static cs.bilkent.joker.engine.region.impl.RegionManagerImplTest.assertBlockingTupleQueueDrainerPool;
import static cs.bilkent.joker.engine.region.impl.RegionManagerImplTest.assertCachedTuplesImplSupplier;
import static cs.bilkent.joker.engine.region.impl.RegionManagerImplTest.assertDefaultOperatorTupleQueue;
import static cs.bilkent.joker.engine.region.impl.RegionManagerImplTest.assertEmptySelfPipelineTupleQueue;
import static cs.bilkent.joker.engine.region.impl.RegionManagerImplTest.assertEmptytOperatorKVStore;
import static cs.bilkent.joker.engine.region.impl.RegionManagerImplTest.assertNonBlockingTupleQueueDrainerPool;
import static cs.bilkent.joker.engine.region.impl.RegionManagerImplTest.assertNonCachedTuplesImplSupplier;
import static cs.bilkent.joker.engine.region.impl.RegionManagerImplTest.assertOperatorDef;
import static cs.bilkent.joker.engine.region.impl.RegionManagerImplTest.assertPartitionedOperatorKVStore;
import static cs.bilkent.joker.engine.region.impl.RegionManagerImplTest.assertPartitionedOperatorTupleQueue;
import cs.bilkent.joker.engine.tuplequeue.OperatorTupleQueue;
import cs.bilkent.joker.engine.tuplequeue.impl.OperatorTupleQueueManagerImpl;
import cs.bilkent.joker.engine.tuplequeue.impl.drainer.GreedyDrainer;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.impl.TuplesImpl;
import cs.bilkent.joker.testutils.AbstractJokerTest;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
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

    private final FlowDeploymentDefFormerImpl flowDeploymentDefFormer = new FlowDeploymentDefFormerImpl( config, idGenerator );

    private final PartitionService partitionService = new PartitionServiceImpl( config );

    private final OperatorKVStoreManagerImpl operatorKVStoreManager = new OperatorKVStoreManagerImpl();

    private final OperatorTupleQueueManagerImpl operatorTupleQueueManager = new OperatorTupleQueueManagerImpl( config,
                                                                                                               new PartitionKeyExtractorFactoryImpl() );

    private final PipelineTransformer pipelineTransformer = new PipelineTransformerImpl( config, operatorTupleQueueManager );

    private final RegionManagerImpl regionManager = new RegionManagerImpl( config,
                                                                           partitionService,
                                                                           operatorKVStoreManager,
                                                                           operatorTupleQueueManager,
                                                                           pipelineTransformer );


    private final int initialReplicaCount;

    private final int rebalancedReplicaCount;

    private final Set<Object> keys = new HashSet<>();

    public RegionRebalancingTest ( final int initialReplicaCount, final int rebalancedReplicaCount )
    {
        this.initialReplicaCount = initialReplicaCount;
        this.rebalancedReplicaCount = rebalancedReplicaCount;
    }

    @Test
    public void shouldRebalancePartitionedStatefulRegion ()
    {
        final FlowExample6 flowExample6 = new FlowExample6();

        final List<RegionDef> regionDefs = regionDefFormer.createRegions( flowExample6.flow );
        final RegionDef regionDef = regionDefs.get( 1 );
        final RegionConfig regionConfig = new RegionConfig( regionDef, asList( 0, 1 ), initialReplicaCount );
        final Region region = regionManager.createRegion( flowExample6.flow, regionConfig );

        final PipelineReplica[] pipelineReplicas = region.getPipelineReplicas( 1 );
        final PartitionDistribution partitionDistribution = partitionService.getPartitionDistributionOrFail( regionDef.getRegionId() );
        for ( int partitionId = 0; partitionId < config.getPartitionServiceConfig().getPartitionCount(); partitionId++ )
        {
            final Tuple tuple = generateTuple( partitionId );
            final int replicaIndex = partitionDistribution.getReplicaIndex( partitionId );
            pipelineReplicas[ replicaIndex ].getSelfPipelineTupleQueue().offer( 0, singletonList( tuple ) );
        }

        final Region rebalancedRegion = regionManager.rebalanceRegion( flowExample6.flow, regionDef.getRegionId(), rebalancedReplicaCount );
        assertPartitionedStatefulRegion( flowExample6, regionDef, rebalancedRegion );
    }

    private void assertPartitionedStatefulRegion ( final FlowExample6 flowExample6, final RegionDef regionDef, final Region region )
    {
        assertNotNull( region );

        for ( int replicaIndex = 0; replicaIndex < region.getConfig().getReplicaCount(); replicaIndex++ )
        {
            final PipelineReplica[] pipelines = region.getReplicaPipelines( replicaIndex );
            assertEquals( 2, pipelines.length );
            final PipelineReplica pipeline0 = pipelines[ 0 ];
            final PipelineReplica pipeline1 = pipelines[ 1 ];
            assertDefaultOperatorTupleQueue( pipeline0, flowExample6.operatorDef1.inputPortCount() );
            assertEmptySelfPipelineTupleQueue( pipeline0 );

            assertEquals( new PipelineReplicaId( new PipelineId( regionDef.getRegionId(), 0 ), replicaIndex ), pipeline0.id() );
            assertEquals( new PipelineReplicaId( new PipelineId( regionDef.getRegionId(), 1 ), replicaIndex ), pipeline1.id() );
            assertEquals( 1, pipeline0.getOperatorCount() );
            assertEquals( 2, pipeline1.getOperatorCount() );

            final OperatorReplica operatorReplica1 = pipeline0.getOperator( 0 );
            assertOperatorDef( operatorReplica1, flowExample6.operatorDef1 );
            assertDefaultOperatorTupleQueue( operatorReplica1, flowExample6.operatorDef1.inputPortCount(), MULTI_THREADED );
            assertEmptytOperatorKVStore( operatorReplica1 );
            assertBlockingTupleQueueDrainerPool( operatorReplica1 );
            assertNonCachedTuplesImplSupplier( operatorReplica1 );

            final OperatorReplica operatorReplica2 = pipeline1.getOperator( 0 );
            assertOperatorDef( operatorReplica2, flowExample6.operatorDef2 );
            assertPartitionedOperatorTupleQueue( operatorReplica2 );
            assertPartitionedOperatorKVStore( operatorReplica2 );
            assertBlockingTupleQueueDrainerPool( operatorReplica2 );
            assertCachedTuplesImplSupplier( operatorReplica2 );

            final OperatorReplica operatorReplica3 = pipeline1.getOperator( 1 );
            assertOperatorDef( operatorReplica3, flowExample6.operatorDef3 );
            assertDefaultOperatorTupleQueue( operatorReplica3, flowExample6.operatorDef3.inputPortCount(), SINGLE_THREADED );
            assertEmptytOperatorKVStore( operatorReplica3 );
            assertNonBlockingTupleQueueDrainerPool( operatorReplica3 );
            assertNonCachedTuplesImplSupplier( operatorReplica3 );
        }

        final PipelineReplica[] pipelineReplicas = region.getPipelineReplicas( 1 );
        for ( PipelineReplica pipelineReplica : pipelineReplicas )
        {
            final OperatorReplica operator = pipelineReplica.getOperator( 0 );

            final GreedyDrainer drainer = new GreedyDrainer( operator.getOperatorDef().inputPortCount() );
            pipelineReplica.getSelfPipelineTupleQueue().drain( drainer );
            final TuplesImpl result = drainer.getResult();
            assertTrue( result == null || result.isEmpty() );
        }

        final PartitionDistribution partitionDistribution = partitionService.getPartitionDistributionOrFail( region.getRegionId() );
        for ( int partitionId = 0; partitionId < partitionService.getPartitionCount(); partitionId++ )
        {
            final int replicaIndex = partitionDistribution.getReplicaIndex( partitionId );
            final OperatorReplica operator = pipelineReplicas[ replicaIndex ].getOperator( 0 );
            final OperatorTupleQueue operatorTupleQueue = operator.getQueue();
            final GreedyDrainer drainer = new GreedyDrainer( operator.getOperatorDef().inputPortCount() );
            operatorTupleQueue.drain( drainer );
            final TuplesImpl result = drainer.getResult();
            if ( result != null && result.isNonEmpty() )
            {
                for ( Tuple tuple : result.getTuplesByDefaultPort() )
                {
                    keys.remove( tuple.get( PARTITION_KEY_FIELD ) );
                }
            }
        }

        assertTrue( keys.isEmpty() );
    }

    @Test( expected = IllegalStateException.class )
    public void shouldNotRebalanceStatelessRegionWhenOperatorTupleQueueIsNonEmpty ()
    {
        final FlowExample2 flowExample2 = new FlowExample2();
        final List<RegionDef> regionDefs = regionDefFormer.createRegions( flowExample2.flow );
        final RegionDef regionDef = regionDefs.get( 1 );
        final RegionConfig regionConfig = new RegionConfig( regionDef, singletonList( 0 ), initialReplicaCount );
        final Region region = regionManager.createRegion( flowExample2.flow, regionConfig );
        final PipelineReplica pipelineReplica = region.getPipelineReplicas( 0 )[ 0 ];
        pipelineReplica.getPipelineTupleQueue().offer( 0, singletonList( new Tuple() ) );

        regionManager.rebalanceRegion( flowExample2.flow, regionDef.getRegionId(), rebalancedReplicaCount );
    }

    @Test
    public void shouldRebalanceStatelessRegion ()
    {
        final FlowExample2 flowExample2 = new FlowExample2();
        final List<RegionDef> regionDefs = regionDefFormer.createRegions( flowExample2.flow );
        final RegionDef regionDef = regionDefs.get( 1 );
        final RegionConfig regionConfig = new RegionConfig( regionDef, singletonList( 0 ), initialReplicaCount );
        regionManager.createRegion( flowExample2.flow, regionConfig );

        final Region rebalancedRegion = regionManager.rebalanceRegion( flowExample2.flow, regionDef.getRegionId(), rebalancedReplicaCount );
        assertRebalancedStatelessRegion( flowExample2, regionDef, rebalancedRegion );
    }

    private void assertRebalancedStatelessRegion ( final FlowExample2 flowExample2, final RegionDef regionDef, final Region region )
    {
        for ( int replicaIndex = 0; replicaIndex < region.getConfig().getReplicaCount(); replicaIndex++ )
        {
            final PipelineReplica[] pipelineReplicas = region.getReplicaPipelines( replicaIndex );
            assertEquals( 1, pipelineReplicas.length );
            final PipelineReplica pipelineReplica0 = pipelineReplicas[ 0 ];
            assertDefaultOperatorTupleQueue( pipelineReplica0, flowExample2.operatorDef1.inputPortCount() );
            assertEmptySelfPipelineTupleQueue( pipelineReplica0 );

            assertEquals( new PipelineReplicaId( new PipelineId( region.getRegionId(), 0 ), replicaIndex ), pipelineReplica0.id() );
            assertEquals( 2, pipelineReplica0.getOperatorCount() );
            final OperatorReplica operatorReplica0_1 = pipelineReplica0.getOperator( 0 );
            final OperatorReplica operatorReplica0_2 = pipelineReplica0.getOperator( 1 );
            assertOperatorDef( operatorReplica0_1, flowExample2.operatorDef1 );
            assertOperatorDef( operatorReplica0_2, flowExample2.operatorDef2 );
            assertDefaultOperatorTupleQueue( operatorReplica0_1, flowExample2.operatorDef1.inputPortCount(), MULTI_THREADED );
            assertDefaultOperatorTupleQueue( operatorReplica0_2, flowExample2.operatorDef2.inputPortCount(), SINGLE_THREADED );
            assertEmptytOperatorKVStore( operatorReplica0_1 );
            assertEmptytOperatorKVStore( operatorReplica0_2 );
            assertBlockingTupleQueueDrainerPool( operatorReplica0_1 );
            assertNonBlockingTupleQueueDrainerPool( operatorReplica0_2 );
            assertCachedTuplesImplSupplier( operatorReplica0_1 );
            assertNonCachedTuplesImplSupplier( operatorReplica0_2 );
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
            final int partitionHash = EXTRACTOR.getPartitionHash( tuple );

            if ( getPartitionId( partitionHash, config.getPartitionServiceConfig().getPartitionCount() ) == partitionId )
            {
                keys.add( i );
                return tuple;
            }

            i++;
        }
    }

}
