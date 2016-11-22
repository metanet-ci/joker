package cs.bilkent.joker.engine.tuplequeue.impl;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

import cs.bilkent.joker.engine.config.JokerConfig;
import static cs.bilkent.joker.engine.config.ThreadingPreference.MULTI_THREADED;
import cs.bilkent.joker.engine.partition.PartitionDistribution;
import cs.bilkent.joker.engine.partition.PartitionKeyExtractor;
import cs.bilkent.joker.engine.partition.PartitionService;
import static cs.bilkent.joker.engine.partition.PartitionUtil.getPartitionId;
import cs.bilkent.joker.engine.partition.impl.PartitionKeyExtractor1;
import cs.bilkent.joker.engine.partition.impl.PartitionKeyExtractorFactoryImpl;
import cs.bilkent.joker.engine.partition.impl.PartitionServiceImpl;
import cs.bilkent.joker.engine.tuplequeue.OperatorTupleQueue;
import cs.bilkent.joker.engine.tuplequeue.impl.drainer.GreedyDrainer;
import cs.bilkent.joker.operator.Operator;
import cs.bilkent.joker.operator.OperatorConfig;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.impl.TuplesImpl;
import cs.bilkent.joker.operator.schema.runtime.OperatorRuntimeSchemaBuilder;
import static cs.bilkent.joker.operator.spec.OperatorType.PARTITIONED_STATEFUL;
import static cs.bilkent.joker.operator.spec.OperatorType.STATELESS;
import cs.bilkent.joker.test.AbstractJokerTest;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


public class OperatorTupleQueueManagerImplTest extends AbstractJokerTest
{

    private static final int REGION_ID = 1;

    private static final OperatorDef STATELESS_OPERATOR = new OperatorDef( "op1",
                                                                           Operator.class,
                                                                           STATELESS,
                                                                           2,
                                                                           1,
                                                                           new OperatorRuntimeSchemaBuilder( 1, 1 ).build(),
                                                                           new OperatorConfig(),
                                                                           Collections.emptyList() );

    private static final OperatorDef PARTITIONED_STATEFUL_OPERATOR = new OperatorDef( "op2",
                                                                                      Operator.class,
                                                                                      PARTITIONED_STATEFUL,
                                                                                      1,
                                                                                      1,
                                                                                      new OperatorRuntimeSchemaBuilder( 1,
                                                                                                                        1 ).addInputField
                                                                                                                                    ( 0,
                                                                                                                                           "field",
                                                                                                                                           Integer.class )
                                                                                                                           .build(),
                                                                                      new OperatorConfig(),
                                                                                      singletonList( "field" ) );

    private static final String PARTITION_KEY_FIELD = "field";

    private static final PartitionKeyExtractor EXTRACTOR = new PartitionKeyExtractor1( singletonList( PARTITION_KEY_FIELD ) );


    private final JokerConfig jokerConfig = new JokerConfig();

    private final PartitionService partitionService = new PartitionServiceImpl( jokerConfig );

    private final OperatorTupleQueueManagerImpl tupleQueueManager = new OperatorTupleQueueManagerImpl( jokerConfig,
                                                                                                       new PartitionKeyExtractorFactoryImpl() );

    private final Set<Object> keys = new HashSet<>();

    @Test
    public void shouldNotCreateOperatorTupleQueueMultipleTimes ()
    {
        tupleQueueManager.createDefaultOperatorTupleQueue( REGION_ID, 1, STATELESS_OPERATOR, MULTI_THREADED );
        try
        {
            tupleQueueManager.createDefaultOperatorTupleQueue( REGION_ID, 1, STATELESS_OPERATOR, MULTI_THREADED );
            fail();
        }
        catch ( IllegalStateException expected )
        {

        }

        final PartitionDistribution partitionDistribution = partitionService.createPartitionDistribution( REGION_ID, 1 );
        tupleQueueManager.createPartitionedOperatorTupleQueues( 1, PARTITIONED_STATEFUL_OPERATOR, partitionDistribution );

        try
        {
            tupleQueueManager.createPartitionedOperatorTupleQueues( 1, PARTITIONED_STATEFUL_OPERATOR, partitionDistribution );
            fail();
        }
        catch ( IllegalStateException expected )
        {

        }
    }

    @Test
    public void shouldCleanOperatorTupleQueueOnRelease ()
    {
        final OperatorTupleQueue operatorTupleQueue = tupleQueueManager.createDefaultOperatorTupleQueue( REGION_ID, 1, STATELESS_OPERATOR,
                                                                                                         MULTI_THREADED );
        operatorTupleQueue.offer( 0, singletonList( new Tuple() ) );
        tupleQueueManager.releaseDefaultOperatorTupleQueue( 1, 1, "op1" );
    }

    @Test
    public void shouldConvertMultiThreadedDefaultOperatorTupleQueueToSingleThreaded ()
    {
        final OperatorTupleQueue operatorTupleQueue = tupleQueueManager.createDefaultOperatorTupleQueue( REGION_ID, 1, STATELESS_OPERATOR,
                                                                                                         MULTI_THREADED );
        final Tuple tuple1 = new Tuple();
        tuple1.set( "key1", "val1" );
        operatorTupleQueue.offer( 0, singletonList( tuple1 ) );
        final Tuple tuple2 = new Tuple();
        tuple2.set( "key2", "val2" );
        operatorTupleQueue.offer( 1, singletonList( tuple2 ) );

        final OperatorTupleQueue operatorTupleQueue2 = tupleQueueManager.switchThreadingPreference( 1, 1, "op1" );
        final GreedyDrainer drainer = new GreedyDrainer( 2 );
        operatorTupleQueue2.drain( drainer );
        final TuplesImpl result = drainer.getResult();
        assertNotNull( result );
        assertEquals( singletonList( tuple1 ), result.getTuples( 0 ) );
        assertEquals( singletonList( tuple2 ), result.getTuples( 1 ) );
    }

    @Test
    public void shouldConvertSingleThreadedDefaultOperatorTupleQueueToMultiThreaded ()
    {
        final OperatorTupleQueue operatorTupleQueue = tupleQueueManager.createDefaultOperatorTupleQueue( 1, 1, STATELESS_OPERATOR,
                                                                                                         MULTI_THREADED );
        final Tuple tuple1 = new Tuple();
        tuple1.set( "key1", "val1" );
        operatorTupleQueue.offer( 0, singletonList( tuple1 ) );
        final Tuple tuple2 = new Tuple();
        tuple2.set( "key2", "val2" );
        operatorTupleQueue.offer( 1, singletonList( tuple2 ) );

        final OperatorTupleQueue operatorTupleQueue2 = tupleQueueManager.switchThreadingPreference( REGION_ID, 1, "op1" );
        final GreedyDrainer drainer = new GreedyDrainer( 2 );
        operatorTupleQueue2.drain( drainer );
        final TuplesImpl result = drainer.getResult();
        assertNotNull( result );
        assertEquals( singletonList( tuple1 ), result.getTuples( 0 ) );
        assertEquals( singletonList( tuple2 ), result.getTuples( 1 ) );
    }

    @Test
    public void shouldShrinkPartitionedOperatorTupleQueues ()
    {
        testRebalancePartitionedOperatorTupleQueues( 4, 2 );
    }

    @Test
    public void shouldExtendPartitionedOperatorTupleQueues ()
    {
        testRebalancePartitionedOperatorTupleQueues( 2, 4 );
    }

    private void testRebalancePartitionedOperatorTupleQueues ( final int initialReplicaCount, final int newReplicaCount )
    {
        final PartitionDistribution partitionDistribution = partitionService.createPartitionDistribution( REGION_ID, initialReplicaCount );
        OperatorTupleQueue[] operatorTupleQueues = tupleQueueManager.createPartitionedOperatorTupleQueues( REGION_ID,
                                                                                                           PARTITIONED_STATEFUL_OPERATOR,
                                                                                                           partitionDistribution );

        final Set<Tuple> tuples = new HashSet<>();
        for ( int partitionId = 0; partitionId < partitionDistribution.getPartitionCount(); partitionId++ )
        {
            final int replicaIndex = partitionDistribution.getReplicaIndex( partitionId );
            final Tuple tuple = generateTuple( partitionId );
            tuples.add( tuple );
            operatorTupleQueues[ replicaIndex ].offer( 0, singletonList( tuple ) );
        }

        final PartitionDistribution newPartitionDistribution = partitionService.rebalancePartitionDistribution( 1, newReplicaCount );
        operatorTupleQueues = tupleQueueManager.rebalancePartitionedOperatorTupleQueues( REGION_ID,
                                                                                         PARTITIONED_STATEFUL_OPERATOR,
                                                                                         partitionDistribution,
                                                                                         newPartitionDistribution );
        assertNotNull( operatorTupleQueues );
        assertEquals( newReplicaCount, operatorTupleQueues.length );

        final GreedyDrainer drainer = new GreedyDrainer( 1 );
        for ( int partitionId = 0; partitionId < partitionDistribution.getPartitionCount(); partitionId++ )
        {
            final int replicaIndex = newPartitionDistribution.getReplicaIndex( partitionId );
            operatorTupleQueues[ replicaIndex ].drain( drainer );
            tuples.removeAll( drainer.getResult().getTuplesByDefaultPort() );
        }

        assertTrue( tuples.isEmpty() );
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

            if ( getPartitionId( partitionHash, partitionService.getPartitionCount() ) == partitionId )
            {
                keys.add( i );
                return tuple;
            }

            i++;
        }
    }

}
