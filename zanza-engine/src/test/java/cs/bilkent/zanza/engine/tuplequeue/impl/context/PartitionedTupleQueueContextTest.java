package cs.bilkent.zanza.engine.tuplequeue.impl.context;

import java.util.List;
import java.util.function.Function;

import org.junit.Before;
import org.junit.Test;

import cs.bilkent.zanza.engine.tuplequeue.TupleQueue;
import cs.bilkent.zanza.engine.tuplequeue.impl.TupleQueueContainer;
import cs.bilkent.zanza.engine.tuplequeue.impl.queue.MultiThreadedTupleQueue;
import cs.bilkent.zanza.operator.Tuple;
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
        final Function<Boolean, TupleQueue> tupleQueueConstructor = capacityCheckEnabled -> new MultiThreadedTupleQueue( TUPLE_QUEUE_SIZE,
                                                                                                                         capacityCheckEnabled );

        final TupleQueueContainer container = new TupleQueueContainer( "op1",
                                                                       2,
                                                                       tupleQueueConstructor );
        tupleQueueContext = new PartitionedTupleQueueContext( "op1",
                                                              2,
                                                              1,
                                                              1,
                                                              tuple -> tuple.get( PARTITION_KEY_FIELD ),
                                                              new TupleQueueContainer[] { container },
                                                              new int[] { 0 } );
    }

    @Test
    public void testOfferNonExceedingTuples ()
    {
        final List<Tuple> tuples = singletonList( new Tuple( PARTITION_KEY_FIELD, "key1" ) );
        tupleQueueContext.offer( 0, tuples );

        final TupleQueue[] tupleQueues = tupleQueueContext.getTupleQueues( tuples );
        assertEquals( tuples, tupleQueues[ 0 ].pollTuples( 1, TIMEOUT_IN_MILLIS ) );
    }

}
