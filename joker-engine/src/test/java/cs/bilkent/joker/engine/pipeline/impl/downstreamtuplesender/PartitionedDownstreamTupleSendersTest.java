package cs.bilkent.joker.engine.pipeline.impl.downstreamtuplesender;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.junit.Test;

import cs.bilkent.joker.engine.partition.PartitionKeyExtractor;
import cs.bilkent.joker.engine.pipeline.DownstreamTupleSenderFailureFlag;
import cs.bilkent.joker.engine.tuplequeue.OperatorQueue;
import cs.bilkent.joker.engine.tuplequeue.TupleQueueDrainer;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.impl.TuplesImpl;
import cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByPort;
import cs.bilkent.joker.partition.impl.PartitionKey;
import cs.bilkent.joker.test.AbstractJokerTest;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PartitionedDownstreamTupleSendersTest extends AbstractJokerTest
{

    private final DownstreamTupleSenderFailureFlag failureFlag = new DownstreamTupleSenderFailureFlag();

    private final TuplesImpl tuples = new TuplesImpl( 10 );

    private final DummyPartitionedOperatorQueue operatorQueue0 = new DummyPartitionedOperatorQueue();

    private final DummyPartitionedOperatorQueue operatorQueue1 = new DummyPartitionedOperatorQueue();

    private final DummyPartitionedOperatorQueue operatorQueue2 = new DummyPartitionedOperatorQueue();

    private final DummyPartitionedOperatorQueue operatorQueue3 = new DummyPartitionedOperatorQueue();

    private final PartitionKeyExtractor partitionKeyExtractor = mock( PartitionKeyExtractor.class );

    private final OperatorQueue[] operatorQueues = new OperatorQueue[] { operatorQueue0, operatorQueue1, operatorQueue2, operatorQueue3 };

    private final int[] partitionDistribution = new int[] { 0, 1, 2, 3, 0, 1, 2, 3 };

    private final int partitionCount = partitionDistribution.length;

    @Test
    public void testPartitionedDownstreamTupleSender1 ()
    {
        final int sourcePortIndex1 = 1;
        final int destinationPortIndex1 = 2;
        final PartitionedDownstreamTupleSender1 tupleSender = new PartitionedDownstreamTupleSender1( failureFlag,
                                                                                                     sourcePortIndex1,
                                                                                                     destinationPortIndex1,
                                                                                                     partitionCount,
                                                                                                     partitionDistribution, operatorQueues,
                                                                                                     partitionKeyExtractor );

        final Tuple tuple = new Tuple();
        tuple.set( "key", "val" );
        tuples.add( sourcePortIndex1, tuple );
        final int replicaIndex = 3;
        when( partitionKeyExtractor.getPartitionHash( tuple ) ).thenReturn( replicaIndex );

        tupleSender.send( tuples );

        assertThat( operatorQueue3.tuplesByPortIndex.get( destinationPortIndex1 ), equalTo( singletonList( tuple ) ) );
    }

    @Test
    public void testPartitionedDownstreamTupleSenderN ()
    {
        final int sourcePortIndex1 = 1, sourcePortIndex2 = 3, sourcePortIndex3 = 5, sourcePortIndex4 = 7;
        final int destinationPortIndex1 = 2, destinationPortIndex2 = 4, destinationPortIndex3 = 6, destinationPortIndex4 = 8;
        final PartitionedDownstreamTupleSenderN tupleSender = new PartitionedDownstreamTupleSenderN( failureFlag,
                                                                                                     new int[] { sourcePortIndex1,
                                                                                                                 sourcePortIndex2,
                                                                                                                 sourcePortIndex3,
                                                                                                                 sourcePortIndex4 },
                                                                                                     new int[] { destinationPortIndex1,
                                                                                                                 destinationPortIndex2,
                                                                                                                 destinationPortIndex3,
                                                                                                                 destinationPortIndex4 },
                                                                                                     partitionCount,
                                                                                                     partitionDistribution, operatorQueues,
                                                                                                     partitionKeyExtractor );

        final Tuple tuple1 = new Tuple();
        tuple1.set( "key1", "val" );
        tuples.add( sourcePortIndex1, tuple1 );
        final Tuple tuple2 = new Tuple();
        tuple2.set( "key2", "val" );
        tuples.add( sourcePortIndex2, tuple2 );
        final Tuple tuple3 = new Tuple();
        tuple3.set( "key3", "val" );
        tuples.add( sourcePortIndex3, tuple3 );
        final Tuple tuple4 = new Tuple();
        tuple4.set( "key4", "val" );
        tuples.add( sourcePortIndex4, tuple4 );
        final int replicaIndex1 = 3, replicaIndex2 = 1, replicaIndex3 = 0, replicaIndex4 = 2;
        when( partitionKeyExtractor.getPartitionHash( tuple1 ) ).thenReturn( replicaIndex1 );
        when( partitionKeyExtractor.getPartitionHash( tuple2 ) ).thenReturn( replicaIndex2 );
        when( partitionKeyExtractor.getPartitionHash( tuple3 ) ).thenReturn( replicaIndex3 );
        when( partitionKeyExtractor.getPartitionHash( tuple4 ) ).thenReturn( replicaIndex4 );

        tupleSender.send( tuples );

        assertThat( operatorQueue3.tuplesByPortIndex.get( destinationPortIndex1 ), equalTo( singletonList( tuple1 ) ) );
        assertThat( operatorQueue1.tuplesByPortIndex.get( destinationPortIndex2 ), equalTo( singletonList( tuple2 ) ) );
        assertThat( operatorQueue0.tuplesByPortIndex.get( destinationPortIndex3 ), equalTo( singletonList( tuple3 ) ) );
        assertThat( operatorQueue2.tuplesByPortIndex.get( destinationPortIndex4 ), equalTo( singletonList( tuple4 ) ) );
    }

    private static class DummyPartitionedOperatorQueue implements OperatorQueue
    {

        private final Map<Integer, List<Tuple>> tuplesByPortIndex = new HashMap<>();

        @Override
        public String getOperatorId ()
        {
            return null;
        }

        @Override
        public int getInputPortCount ()
        {
            return 0;
        }

        @Override
        public int offer ( final int portIndex, final List<Tuple> tuples )
        {
            tuplesByPortIndex.computeIfAbsent( portIndex, ArrayList::new ).addAll( tuples );
            return tuples.size();
        }

        @Override
        public int offer ( final int portIndex, final List<Tuple> tuples, final int fromIndex )
        {
            tuplesByPortIndex.computeIfAbsent( portIndex, ArrayList::new ).addAll( tuples.subList( fromIndex, tuples.size() ) );
            return tuples.size() - fromIndex;
        }

        @Override
        public void drain ( final boolean maySkipBlocking,
                            final TupleQueueDrainer drainer,
                            final Function<PartitionKey, TuplesImpl> tuplesSupplier )
        {

        }

        @Override
        public void clear ()
        {

        }

        @Override
        public void setTupleCounts ( final int[] tupleCounts, final TupleAvailabilityByPort tupleAvailabilityByPort )
        {

        }

        @Override
        public boolean isEmpty ()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void ensureCapacity ( final int capacity )
        {

        }

    }

}
