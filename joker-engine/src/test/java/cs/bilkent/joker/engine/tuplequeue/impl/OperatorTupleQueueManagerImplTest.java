package cs.bilkent.joker.engine.tuplequeue.impl;

import java.util.Collections;

import org.junit.Test;

import cs.bilkent.joker.engine.config.JokerConfig;
import cs.bilkent.joker.engine.config.ThreadingPreference;
import static cs.bilkent.joker.engine.config.ThreadingPreference.MULTI_THREADED;
import cs.bilkent.joker.engine.partition.PartitionDistribution;
import cs.bilkent.joker.engine.partition.PartitionService;
import cs.bilkent.joker.engine.partition.PartitionServiceImpl;
import cs.bilkent.joker.engine.partition.impl.PartitionKeyExtractorFactoryImpl;
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
import cs.bilkent.joker.testutils.AbstractJokerTest;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;


public class OperatorTupleQueueManagerImplTest extends AbstractJokerTest
{


    private final JokerConfig jokerConfig = new JokerConfig();

    private final PartitionService partitionService = new PartitionServiceImpl( jokerConfig );

    private final OperatorTupleQueueManagerImpl tupleQueueManager = new OperatorTupleQueueManagerImpl( jokerConfig,
                                                                                                       new PartitionKeyExtractorFactoryImpl() );

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotCreateOperatorTupleQueueWithoutOperatorDef ()
    {
        tupleQueueManager.createDefaultOperatorTupleQueue( 1, 1, null, ThreadingPreference.SINGLE_THREADED );
    }

    @Test
    public void shouldNotCreateOperatorTupleQueueMultipleTimes ()
    {
        final OperatorDef operatorDef1 = new OperatorDef( "op1",
                                                          Operator.class,
                                                          STATELESS,
                                                          1,
                                                          1,
                                                          new OperatorRuntimeSchemaBuilder( 1, 1 ).build(),
                                                          new OperatorConfig(),
                                                          Collections.emptyList() );

        tupleQueueManager.createDefaultOperatorTupleQueue( 1, 1, operatorDef1, MULTI_THREADED );
        try
        {
            tupleQueueManager.createDefaultOperatorTupleQueue( 1, 1, operatorDef1, MULTI_THREADED );
            fail();
        }
        catch ( IllegalStateException expected )
        {

        }

        final OperatorDef operatorDef2 = new OperatorDef( "op1",
                                                          Operator.class,
                                                          PARTITIONED_STATEFUL,
                                                          1,
                                                          1,
                                                          new OperatorRuntimeSchemaBuilder( 1, 1 ).addInputField( 0,
                                                                                                                  "field",
                                                                                                                  Integer.class ).build(),
                                                          new OperatorConfig(),
                                                          singletonList( "field" ) );

        final PartitionDistribution partitionDistribution = partitionService.createPartitionDistribution( 1, 1 );
        tupleQueueManager.createPartitionedOperatorTupleQueue( 1, operatorDef2, partitionDistribution );

        try
        {
            tupleQueueManager.createPartitionedOperatorTupleQueue( 1, operatorDef2, partitionDistribution );
            fail();
        }
        catch ( IllegalStateException expected )
        {

        }
    }

    @Test
    public void shouldCleanOperatorTupleQueueOnRelease ()
    {
        final OperatorDef operatorDef = new OperatorDef( "op1",
                                                         Operator.class, STATELESS,
                                                         1,
                                                         1,
                                                         new OperatorRuntimeSchemaBuilder( 1, 1 ).build(),
                                                         new OperatorConfig(),
                                                         Collections.emptyList() );
        final OperatorTupleQueue operatorTupleQueue = tupleQueueManager.createDefaultOperatorTupleQueue( 1,
                                                                                                         1,
                                                                                                         operatorDef,
                                                                                                         MULTI_THREADED );
        operatorTupleQueue.offer( 0, singletonList( new Tuple() ) );
        tupleQueueManager.releaseDefaultOperatorTupleQueue( 1, 1, "op1" );
    }

    @Test
    public void shouldConvertMultiThreadedDefaultOperatorTupleQueueToSingleThreaded ()
    {
        final OperatorDef operatorDef = new OperatorDef( "op1",
                                                         Operator.class, STATELESS,
                                                         2,
                                                         1,
                                                         new OperatorRuntimeSchemaBuilder( 1, 1 ).build(),
                                                         new OperatorConfig(),
                                                         Collections.emptyList() );

        final OperatorTupleQueue operatorTupleQueue = tupleQueueManager.createDefaultOperatorTupleQueue( 1,
                                                                                                         1,
                                                                                                         operatorDef,
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
        final OperatorDef operatorDef = new OperatorDef( "op1",
                                                         Operator.class, STATELESS, 2,
                                                         1,
                                                         new OperatorRuntimeSchemaBuilder( 1, 1 ).build(),
                                                         new OperatorConfig(),
                                                         Collections.emptyList() );

        final OperatorTupleQueue operatorTupleQueue = tupleQueueManager.createDefaultOperatorTupleQueue( 1,
                                                                                                         1,
                                                                                                         operatorDef,
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

}
