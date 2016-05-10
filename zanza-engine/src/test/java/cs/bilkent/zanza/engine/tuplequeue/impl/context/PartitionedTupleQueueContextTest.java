package cs.bilkent.zanza.engine.tuplequeue.impl.context;

import java.util.List;

import org.junit.Before;
import org.junit.Test;

import static cs.bilkent.zanza.engine.TestUtils.assertTrueEventually;
import static cs.bilkent.zanza.engine.TestUtils.spawnThread;
import cs.bilkent.zanza.engine.config.ThreadingPreference;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueue;
import cs.bilkent.zanza.engine.tuplequeue.impl.queue.MultiThreadedTupleQueue;
import cs.bilkent.zanza.operator.Tuple;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;

public class PartitionedTupleQueueContextTest
{

    private static final int TIMEOUT_IN_MILLIS = 2000;

    private static final int TUPLE_QUEUE_SIZE = 3;

    private static final String PARTITION_KEY_FIELD = "key";


    private PartitionedTupleQueueContext tupleQueueContext;

    @Before
    public void init ()
    {
        tupleQueueContext = new PartitionedTupleQueueContext( "op1",
                                                              2,
                                                              ThreadingPreference.MULTI_THREADED,
                                                              tuple -> tuple.get( PARTITION_KEY_FIELD ),
                                                              capacityCheckEnabled -> new MultiThreadedTupleQueue( TUPLE_QUEUE_SIZE,
                                                                                                                   capacityCheckEnabled ) );
    }

    @Test
    public void testOfferNonExceedingTuples ()
    {
        final List<Tuple> tuples = singletonList( new Tuple( PARTITION_KEY_FIELD, "key1" ) );
        tupleQueueContext.offer( 0, tuples );

        final TupleQueue[] tupleQueues = tupleQueueContext.getTupleQueues( tuples );
        assertEquals( tuples, tupleQueues[ 0 ].pollTuples( 1, TIMEOUT_IN_MILLIS ) );
    }

    @Test
    public void testOfferExceedingTuplesAfterCapacityCheckDisabled ()
    {
        final List<Tuple> tuples = asList( new Tuple( PARTITION_KEY_FIELD, "key1" ),
                                           new Tuple( PARTITION_KEY_FIELD, "key1" ),
                                           new Tuple( PARTITION_KEY_FIELD, "key1" ),
                                           new Tuple( PARTITION_KEY_FIELD, "key1" ) );

        spawnThread( () -> tupleQueueContext.offer( 0, tuples ) );

        tupleQueueContext.disableCapacityCheck( 0 );

        assertTrueEventually( () -> {
            final TupleQueue[] tupleQueues = tupleQueueContext.getTupleQueues( tuples );
            assertEquals( tuples, tupleQueues[ 0 ].pollTuples( 4, TIMEOUT_IN_MILLIS ) );
        } );
    }

}
