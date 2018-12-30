package cs.bilkent.joker.engine.tuplequeue.impl;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

import cs.bilkent.joker.engine.config.JokerConfig;
import static cs.bilkent.joker.engine.config.ThreadingPref.MULTI_THREADED;
import cs.bilkent.joker.engine.partition.PartitionDistribution;
import cs.bilkent.joker.engine.partition.PartitionKeyExtractor;
import cs.bilkent.joker.engine.partition.PartitionService;
import static cs.bilkent.joker.engine.partition.PartitionUtil.getPartitionId;
import cs.bilkent.joker.engine.partition.impl.PartitionKeyExtractor1;
import cs.bilkent.joker.engine.partition.impl.PartitionKeyExtractorFactoryImpl;
import cs.bilkent.joker.engine.partition.impl.PartitionServiceImpl;
import cs.bilkent.joker.engine.tuplequeue.OperatorQueue;
import cs.bilkent.joker.engine.tuplequeue.TupleQueueDrainer;
import static cs.bilkent.joker.engine.tuplequeue.impl.drainer.NonBlockingMultiPortDisjunctiveDrainer.newGreedyDrainer;
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


public class OperatorQueueManagerImplTest extends AbstractJokerTest
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
                                                                                                                        1 ).addInputField( 0,
                                                                                                                                           "field",
                                                                                                                                           Integer.class )
                                                                                                                           .build(),
                                                                                      new OperatorConfig(),
                                                                                      singletonList( "field" ) );

    private static final String PARTITION_KEY_FIELD = "field";

    private static final PartitionKeyExtractor EXTRACTOR = new PartitionKeyExtractor1( singletonList( PARTITION_KEY_FIELD ) );


    private final JokerConfig jokerConfig = new JokerConfig();

    private final PartitionService partitionService = new PartitionServiceImpl( jokerConfig );

    private final OperatorQueueManagerImpl operatorQueueManager = new OperatorQueueManagerImpl( jokerConfig,
                                                                                                new PartitionKeyExtractorFactoryImpl() );

    private final Set<Object> keys = new HashSet<>();

    @Test
    public void shouldNotCreateOperatorQueueMultipleTimes ()
    {
        operatorQueueManager.createDefaultQueue( REGION_ID, STATELESS_OPERATOR, 1, MULTI_THREADED );
        try
        {
            operatorQueueManager.createDefaultQueue( REGION_ID, STATELESS_OPERATOR, 1, MULTI_THREADED );
            fail();
        }
        catch ( IllegalStateException ignored )
        {

        }

        final PartitionDistribution partitionDistribution = partitionService.createPartitionDistribution( REGION_ID, 1 );
        operatorQueueManager.createPartitionedQueues( 1, PARTITIONED_STATEFUL_OPERATOR, partitionDistribution );

        try
        {
            operatorQueueManager.createPartitionedQueues( 1, PARTITIONED_STATEFUL_OPERATOR, partitionDistribution );
            fail();
        }
        catch ( IllegalStateException ignored )
        {

        }
    }

    @Test
    public void shouldCleanOperatorQueueOnRelease ()
    {
        final OperatorQueue operatorQueue = operatorQueueManager.createDefaultQueue( REGION_ID, STATELESS_OPERATOR, 1, MULTI_THREADED );
        operatorQueue.offer( 0, singletonList( new Tuple() ) );
        operatorQueueManager.releaseDefaultQueue( 1, "op1", 1 );
    }

    @Test
    public void shouldConvertMultiThreadedDefaultOperatorQueueToSingleThreaded ()
    {
        final OperatorQueue operatorQueue = operatorQueueManager.createDefaultQueue( REGION_ID, STATELESS_OPERATOR, 1, MULTI_THREADED );
        final Tuple tuple1 = Tuple.of( "key1", "val1" );
        operatorQueue.offer( 0, singletonList( tuple1 ) );
        final Tuple tuple2 = Tuple.of( "key2", "val2" );
        operatorQueue.offer( 1, singletonList( tuple2 ) );

        final OperatorQueue operatorQueue2 = operatorQueueManager.switchThreadingPref( 1, "op1", 1 );

        final TuplesImpl result = new TuplesImpl( 2 );
        final TupleQueueDrainer drainer = newGreedyDrainer( STATELESS_OPERATOR.getId(), 2, Integer.MAX_VALUE );
        operatorQueue2.drain( drainer, key -> result );
        assertEquals( singletonList( tuple1 ), result.getTuples( 0 ) );
        assertEquals( singletonList( tuple2 ), result.getTuples( 1 ) );
    }

    @Test
    public void shouldConvertSingleThreadedDefaultOperatorQueueToMultiThreaded ()
    {
        final OperatorQueue operatorQueue = operatorQueueManager.createDefaultQueue( 1, STATELESS_OPERATOR, 1, MULTI_THREADED );
        final Tuple tuple1 = Tuple.of( "key1", "val1" );
        operatorQueue.offer( 0, singletonList( tuple1 ) );
        final Tuple tuple2 = Tuple.of( "key2", "val2" );
        operatorQueue.offer( 1, singletonList( tuple2 ) );

        final OperatorQueue operatorQueue2 = operatorQueueManager.switchThreadingPref( REGION_ID, "op1", 1 );

        final TuplesImpl result = new TuplesImpl( 2 );
        final TupleQueueDrainer drainer = newGreedyDrainer( STATELESS_OPERATOR.getId(), 2, Integer.MAX_VALUE );
        operatorQueue2.drain( drainer, key -> result );
        assertEquals( singletonList( tuple1 ), result.getTuples( 0 ) );
        assertEquals( singletonList( tuple2 ), result.getTuples( 1 ) );
    }

    @Test
    public void shouldShrinkPartitionedOperatorQueues ()
    {
        testRebalancePartitionedOperatorQueues( 4, 2 );
    }

    @Test
    public void shouldExtendPartitionedOperatorQueues ()
    {
        testRebalancePartitionedOperatorQueues( 2, 4 );
    }

    private void testRebalancePartitionedOperatorQueues ( final int initialReplicaCount, final int newReplicaCount )
    {
        final PartitionDistribution partitionDistribution = partitionService.createPartitionDistribution( REGION_ID, initialReplicaCount );
        OperatorQueue[] operatorQueues = operatorQueueManager.createPartitionedQueues( REGION_ID,
                                                                                       PARTITIONED_STATEFUL_OPERATOR,
                                                                                       partitionDistribution );

        final Set<Tuple> tuples = new HashSet<>();
        for ( int partitionId = 0; partitionId < partitionDistribution.getPartitionCount(); partitionId++ )
        {
            final int replicaIndex = partitionDistribution.getReplicaIndex( partitionId );
            final Tuple tuple = generateTuple( partitionId );
            tuples.add( tuple );
            operatorQueues[ replicaIndex ].offer( 0, singletonList( tuple ) );
        }

        final PartitionDistribution newPartitionDistribution = partitionService.rebalancePartitionDistribution( 1, newReplicaCount );
        operatorQueues = operatorQueueManager.rebalancePartitionedQueues( REGION_ID,
                                                                          PARTITIONED_STATEFUL_OPERATOR,
                                                                          partitionDistribution,
                                                                          newPartitionDistribution );
        assertNotNull( operatorQueues );
        assertEquals( newReplicaCount, operatorQueues.length );

        final TupleQueueDrainer drainer = newGreedyDrainer( PARTITIONED_STATEFUL_OPERATOR.getId(), 1, Integer.MAX_VALUE );
        for ( int partitionId = 0; partitionId < partitionDistribution.getPartitionCount(); partitionId++ )
        {
            final TuplesImpl result = new TuplesImpl( 1 );
            final int replicaIndex = newPartitionDistribution.getReplicaIndex( partitionId );
            operatorQueues[ replicaIndex ].drain( drainer, key -> result );
            tuples.removeAll( result.getTuplesByDefaultPort() );
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
