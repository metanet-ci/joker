package cs.bilkent.joker.engine.pipeline.impl;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import cs.bilkent.joker.engine.pipeline.impl.downstreamtuplesender.DownstreamTupleSender1;
import cs.bilkent.joker.engine.pipeline.impl.downstreamtuplesender.DownstreamTupleSender2;
import cs.bilkent.joker.engine.pipeline.impl.downstreamtuplesender.DownstreamTupleSender3;
import cs.bilkent.joker.engine.pipeline.impl.downstreamtuplesender.DownstreamTupleSender4;
import cs.bilkent.joker.engine.pipeline.impl.downstreamtuplesender.DownstreamTupleSenderN;
import cs.bilkent.joker.engine.tuplequeue.TupleQueueContext;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.impl.TuplesImpl;
import cs.bilkent.joker.testutils.AbstractJokerTest;
import static java.util.Collections.singletonList;
import static org.mockito.Mockito.verify;

@RunWith( MockitoJUnitRunner.class )
public class DownstreamTupleSendersTest extends AbstractJokerTest
{

    private final TuplesImpl tuples = new TuplesImpl( 10 );

    @Mock
    private TupleQueueContext tupleQueueContext;

    @Test
    public void testDownstreamTupleSender1 ()
    {
        final DownstreamTupleSender1 tupleSender = new DownstreamTupleSender1( 1, 2, tupleQueueContext );
        final Tuple tuple = new Tuple();
        tuple.set( "key", "val" );
        tuples.add( 1, tuple );

        tupleSender.send( tuples );

        final Tuple expected = new Tuple();
        expected.set( "key", "val" );
        verify( tupleQueueContext ).offer( 2, singletonList( expected ) );
    }

    @Test
    public void testDownstreamTupleSender2 ()
    {
        final DownstreamTupleSender2 tupleSender = new DownstreamTupleSender2( 1, 2, 3, 4, tupleQueueContext );
        final Tuple tuple1 = new Tuple();
        tuple1.set( "key1", "val" );
        tuples.add( 1, tuple1 );
        final Tuple tuple2 = new Tuple();
        tuple2.set( "key2", "val" );
        tuples.add( 3, tuple2 );

        tupleSender.send( tuples );

        final Tuple expected1 = new Tuple();
        expected1.set( "key1", "val" );
        verify( tupleQueueContext ).offer( 2, singletonList( expected1 ) );
        final Tuple expected2 = new Tuple();
        expected2.set( "key2", "val" );
        verify( tupleQueueContext ).offer( 4, singletonList( expected2 ) );
    }

    @Test
    public void testDownstreamTupleSender3 ()
    {
        final DownstreamTupleSender3 tupleSender = new DownstreamTupleSender3( 1, 2, 3, 4, 5, 6, tupleQueueContext );
        final Tuple tuple1 = new Tuple();
        tuple1.set( "key1", "val" );
        tuples.add( 1, tuple1 );
        final Tuple tuple2 = new Tuple();
        tuple2.set( "key2", "val" );
        tuples.add( 3, tuple2 );
        final Tuple tuple3 = new Tuple();
        tuple3.set( "key3", "val" );
        tuples.add( 5, tuple3 );

        tupleSender.send( tuples );

        final Tuple expected1 = new Tuple();
        expected1.set( "key1", "val" );
        verify( tupleQueueContext ).offer( 2, singletonList( expected1 ) );
        final Tuple expected2 = new Tuple();
        expected2.set( "key2", "val" );
        verify( tupleQueueContext ).offer( 4, singletonList( expected2 ) );
        final Tuple expected3 = new Tuple();
        expected3.set( "key3", "val" );
        verify( tupleQueueContext ).offer( 6, singletonList( expected3 ) );
    }

    @Test
    public void testDownstreamTupleSender4 ()
    {
        final DownstreamTupleSender4 tupleSender = new DownstreamTupleSender4( 1, 2, 3, 4, 5, 6, 7, 8, tupleQueueContext );
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

        tupleSender.send( tuples );

        final Tuple expected1 = new Tuple();
        expected1.set( "key1", "val" );
        verify( tupleQueueContext ).offer( 2, singletonList( expected1 ) );
        final Tuple expected2 = new Tuple();
        expected2.set( "key2", "val" );
        verify( tupleQueueContext ).offer( 4, singletonList( expected2 ) );
        final Tuple expected3 = new Tuple();
        expected3.set( "key3", "val" );
        verify( tupleQueueContext ).offer( 6, singletonList( expected3 ) );
        final Tuple expected4 = new Tuple();
        expected4.set( "key4", "val" );
        verify( tupleQueueContext ).offer( 8, singletonList( expected4 ) );
    }

    @Test
    public void testDownstreamTupleSenderN ()
    {
        final DownstreamTupleSenderN tupleSender = new DownstreamTupleSenderN( new int[] { 1, 3, 5, 7, 9 },
                                                                               new int[] { 2, 4, 6, 8, 10 },
                                                                               tupleQueueContext );
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

        tupleSender.send( tuples );

        final Tuple expected1 = new Tuple();
        expected1.set( "key1", "val" );
        verify( tupleQueueContext ).offer( 2, singletonList( expected1 ) );
        final Tuple expected2 = new Tuple();
        expected2.set( "key2", "val" );
        verify( tupleQueueContext ).offer( 4, singletonList( expected2 ) );
        final Tuple expected3 = new Tuple();
        expected3.set( "key3", "val" );
        verify( tupleQueueContext ).offer( 6, singletonList( expected3 ) );
        final Tuple expected4 = new Tuple();
        expected4.set( "key4", "val" );
        verify( tupleQueueContext ).offer( 8, singletonList( expected4 ) );
        final Tuple expected5 = new Tuple();
        expected5.set( "key5", "val" );
        verify( tupleQueueContext ).offer( 10, singletonList( expected5 ) );
    }

}
