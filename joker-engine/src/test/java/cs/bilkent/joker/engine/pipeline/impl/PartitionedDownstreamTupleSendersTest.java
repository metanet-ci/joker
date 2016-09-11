package cs.bilkent.joker.engine.pipeline.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import cs.bilkent.joker.engine.partition.PartitionKeyFunction;
import cs.bilkent.joker.engine.pipeline.impl.downstreamtuplesender.PartitionedDownstreamTupleSender1;
import cs.bilkent.joker.engine.pipeline.impl.downstreamtuplesender.PartitionedDownstreamTupleSender2;
import cs.bilkent.joker.engine.pipeline.impl.downstreamtuplesender.PartitionedDownstreamTupleSender3;
import cs.bilkent.joker.engine.pipeline.impl.downstreamtuplesender.PartitionedDownstreamTupleSender4;
import cs.bilkent.joker.engine.pipeline.impl.downstreamtuplesender.PartitionedDownstreamTupleSenderN;
import cs.bilkent.joker.engine.tuplequeue.TupleQueueContext;
import cs.bilkent.joker.engine.tuplequeue.TupleQueueDrainer;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.impl.TuplesImpl;
import cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByPort;
import cs.bilkent.joker.testutils.AbstractJokerTest;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PartitionedDownstreamTupleSendersTest extends AbstractJokerTest
{


    private final TuplesImpl tuples = new TuplesImpl( 10 );

    private final DummyPartitionedTupleQueueContext tupleQueueContext0 = new DummyPartitionedTupleQueueContext();

    private final DummyPartitionedTupleQueueContext tupleQueueContext1 = new DummyPartitionedTupleQueueContext();

    private final DummyPartitionedTupleQueueContext tupleQueueContext2 = new DummyPartitionedTupleQueueContext();

    private final DummyPartitionedTupleQueueContext tupleQueueContext3 = new DummyPartitionedTupleQueueContext();

    private final PartitionKeyFunction partitionKeyFunction = mock( PartitionKeyFunction.class );

    private final TupleQueueContext[] tupleQueueContexts = new TupleQueueContext[] { tupleQueueContext0,
                                                                                     tupleQueueContext1,
                                                                                     tupleQueueContext2,
                                                                                     tupleQueueContext3 };

    private final int[] partitionDistribution = new int[] { 0, 1, 2, 3, 0, 1, 2, 3 };

    private final int partitionCount = partitionDistribution.length;

    @Test
    public void testPartitionedDownstreamTupleSender1 ()
    {
        final PartitionedDownstreamTupleSender1 tupleSender = new PartitionedDownstreamTupleSender1( 1,
                                                                                                     2,
                                                                                                     partitionCount,
                                                                                                     partitionDistribution,
                                                                                                     tupleQueueContexts,
                                                                                                     partitionKeyFunction );

        final Tuple tuple = new Tuple();
        tuple.set( "key", "val" );
        tuples.add( 1, tuple );
        final int replicaIndex = 3;
        when( partitionKeyFunction.getPartitionHash( tuple ) ).thenReturn( replicaIndex );

        tupleSender.send( tuples );

        assertThat( tupleQueueContext3.tuplesByPortIndex.get( 2 ), equalTo( singletonList( tuple ) ) );
    }

    @Test
    public void testPartitionedDownstreamTupleSender2 ()
    {
        final PartitionedDownstreamTupleSender2 tupleSender = new PartitionedDownstreamTupleSender2( 1,
                                                                                                     2,
                                                                                                     3,
                                                                                                     4,
                                                                                                     partitionCount,
                                                                                                     partitionDistribution,
                                                                                                     tupleQueueContexts,
                                                                                                     partitionKeyFunction );

        final Tuple tuple1 = new Tuple();
        tuple1.set( "key1", "val" );
        tuples.add( 1, tuple1 );
        final Tuple tuple2 = new Tuple();
        tuple2.set( "key2", "val" );
        tuples.add( 3, tuple2 );
        final int replicaIndex1 = 3, replicaIndex2 = 1;
        when( partitionKeyFunction.getPartitionHash( tuple1 ) ).thenReturn( replicaIndex1 );
        when( partitionKeyFunction.getPartitionHash( tuple2 ) ).thenReturn( replicaIndex2 );

        tupleSender.send( tuples );

        assertThat( tupleQueueContext3.tuplesByPortIndex.get( 2 ), equalTo( singletonList( tuple1 ) ) );
        assertThat( tupleQueueContext1.tuplesByPortIndex.get( 4 ), equalTo( singletonList( tuple2 ) ) );
    }

    @Test
    public void testPartitionedDownstreamTupleSender3 ()
    {
        final PartitionedDownstreamTupleSender3 tupleSender = new PartitionedDownstreamTupleSender3( 1,
                                                                                                     2,
                                                                                                     3,
                                                                                                     4,
                                                                                                     5,
                                                                                                     6,
                                                                                                     partitionCount,
                                                                                                     partitionDistribution,
                                                                                                     tupleQueueContexts,
                                                                                                     partitionKeyFunction );

        final Tuple tuple1 = new Tuple();
        tuple1.set( "key1", "val" );
        tuples.add( 1, tuple1 );
        final Tuple tuple2 = new Tuple();
        tuple2.set( "key2", "val" );
        tuples.add( 3, tuple2 );
        final Tuple tuple3 = new Tuple();
        tuple3.set( "key3", "val" );
        tuples.add( 5, tuple3 );
        final int replicaIndex1 = 3, replicaIndex2 = 1, replicaIndex3 = 0;
        when( partitionKeyFunction.getPartitionHash( tuple1 ) ).thenReturn( replicaIndex1 );
        when( partitionKeyFunction.getPartitionHash( tuple2 ) ).thenReturn( replicaIndex2 );
        when( partitionKeyFunction.getPartitionHash( tuple3 ) ).thenReturn( replicaIndex3 );

        tupleSender.send( tuples );

        assertThat( tupleQueueContext3.tuplesByPortIndex.get( 2 ), equalTo( singletonList( tuple1 ) ) );
        assertThat( tupleQueueContext1.tuplesByPortIndex.get( 4 ), equalTo( singletonList( tuple2 ) ) );
        assertThat( tupleQueueContext0.tuplesByPortIndex.get( 6 ), equalTo( singletonList( tuple3 ) ) );
    }

    @Test
    public void testPartitionedDownstreamTupleSender4 ()
    {
        final PartitionedDownstreamTupleSender4 tupleSender = new PartitionedDownstreamTupleSender4( 1,
                                                                                                     2,
                                                                                                     3,
                                                                                                     4,
                                                                                                     5,
                                                                                                     6,
                                                                                                     7,
                                                                                                     8,
                                                                                                     partitionCount,
                                                                                                     partitionDistribution,
                                                                                                     tupleQueueContexts,
                                                                                                     partitionKeyFunction );

        final Tuple tuple1 = new Tuple();
        tuple1.set( "key1", "val" );
        tuples.add( 1, tuple1 );
        final Tuple tuple2 = new Tuple();
        tuple2.set( "key2", "val" );
        tuples.add( 3, tuple2 );
        final Tuple tuple3 = new Tuple();
        tuple3.set( "key3", "val" );
        tuples.add( 5, tuple3 );
        final Tuple tuple4 = new Tuple();
        tuple4.set( "key4", "val" );
        tuples.add( 7, tuple4 );
        final int replicaIndex1 = 3, replicaIndex2 = 1, replicaIndex3 = 0, replicaIndex4 = 2;
        when( partitionKeyFunction.getPartitionHash( tuple1 ) ).thenReturn( replicaIndex1 );
        when( partitionKeyFunction.getPartitionHash( tuple2 ) ).thenReturn( replicaIndex2 );
        when( partitionKeyFunction.getPartitionHash( tuple3 ) ).thenReturn( replicaIndex3 );
        when( partitionKeyFunction.getPartitionHash( tuple4 ) ).thenReturn( replicaIndex4 );

        tupleSender.send( tuples );

        assertThat( tupleQueueContext3.tuplesByPortIndex.get( 2 ), equalTo( singletonList( tuple1 ) ) );
        assertThat( tupleQueueContext1.tuplesByPortIndex.get( 4 ), equalTo( singletonList( tuple2 ) ) );
        assertThat( tupleQueueContext0.tuplesByPortIndex.get( 6 ), equalTo( singletonList( tuple3 ) ) );
        assertThat( tupleQueueContext2.tuplesByPortIndex.get( 8 ), equalTo( singletonList( tuple4 ) ) );
    }

    @Test
    public void testPartitionedDownstreamTupleSenderN ()
    {
        final PartitionedDownstreamTupleSenderN tupleSender = new PartitionedDownstreamTupleSenderN( new int[] { 1, 3, 5, 7, 9 },
                                                                                                     new int[] { 2, 4, 6, 8, 10 },
                                                                                                     partitionCount,
                                                                                                     partitionDistribution,
                                                                                                     tupleQueueContexts,
                                                                                                     partitionKeyFunction );

        final Tuple tuple1 = new Tuple();
        tuple1.set( "key1", "val" );
        tuples.add( 1, tuple1 );
        final Tuple tuple2 = new Tuple();
        tuple2.set( "key2", "val" );
        tuples.add( 3, tuple2 );
        final Tuple tuple3 = new Tuple();
        tuple3.set( "key3", "val" );
        tuples.add( 5, tuple3 );
        final Tuple tuple4 = new Tuple();
        tuple4.set( "key4", "val" );
        tuples.add( 7, tuple4 );
        final Tuple tuple5 = new Tuple();
        tuple5.set( "key5", "val" );
        tuples.add( 9, tuple5 );
        final int replicaIndex1 = 3, replicaIndex2 = 1, replicaIndex3 = 0, replicaIndex4 = 2, replicaIndex5 = 3;
        when( partitionKeyFunction.getPartitionHash( tuple1 ) ).thenReturn( replicaIndex1 );
        when( partitionKeyFunction.getPartitionHash( tuple2 ) ).thenReturn( replicaIndex2 );
        when( partitionKeyFunction.getPartitionHash( tuple3 ) ).thenReturn( replicaIndex3 );
        when( partitionKeyFunction.getPartitionHash( tuple4 ) ).thenReturn( replicaIndex4 );
        when( partitionKeyFunction.getPartitionHash( tuple5 ) ).thenReturn( replicaIndex5 );

        tupleSender.send( tuples );

        assertThat( tupleQueueContext3.tuplesByPortIndex.get( 2 ), equalTo( singletonList( tuple1 ) ) );
        assertThat( tupleQueueContext1.tuplesByPortIndex.get( 4 ), equalTo( singletonList( tuple2 ) ) );
        assertThat( tupleQueueContext0.tuplesByPortIndex.get( 6 ), equalTo( singletonList( tuple3 ) ) );
        assertThat( tupleQueueContext2.tuplesByPortIndex.get( 8 ), equalTo( singletonList( tuple4 ) ) );
        assertThat( tupleQueueContext3.tuplesByPortIndex.get( 10 ), equalTo( singletonList( tuple5 ) ) );
    }

    private static class DummyPartitionedTupleQueueContext implements TupleQueueContext
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
        public void offer ( final int portIndex, final List<Tuple> tuples )
        {
            tuplesByPortIndex.computeIfAbsent( portIndex, ArrayList::new ).addAll( tuples );
        }

        @Override
        public int tryOffer ( final int portIndex, final List<Tuple> tuples, final long timeout, final TimeUnit unit )
        {
            return 0;
        }

        @Override
        public void forceOffer ( final int portIndex, final List<Tuple> tuples )
        {

        }

        @Override
        public void drain ( final TupleQueueDrainer drainer )
        {

        }

        @Override
        public void ensureCapacity ( final int portIndex, final int capacity )
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
        public void enableCapacityCheck ( final int portIndex )
        {

        }

        @Override
        public void disableCapacityCheck ( final int portIndex )
        {

        }

        @Override
        public boolean isCapacityCheckEnabled ( final int portIndex )
        {
            return false;
        }

        @Override
        public boolean isOverloaded ()
        {
            return false;
        }

    }

}
