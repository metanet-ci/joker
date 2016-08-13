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
        tuples.add( 1, new Tuple( "key", "val" ) );

        tupleSender.send( tuples );

        verify( tupleQueueContext ).offer( 2, singletonList( new Tuple( "key", "val" ) ) );
    }

    @Test
    public void testDownstreamTupleSender2 ()
    {
        final DownstreamTupleSender2 tupleSender = new DownstreamTupleSender2( 1, 2, 3, 4, tupleQueueContext );
        tuples.add( 1, new Tuple( "key1", "val" ) );
        tuples.add( 3, new Tuple( "key2", "val" ) );

        tupleSender.send( tuples );

        verify( tupleQueueContext ).offer( 2, singletonList( new Tuple( "key1", "val" ) ) );
        verify( tupleQueueContext ).offer( 4, singletonList( new Tuple( "key2", "val" ) ) );
    }

    @Test
    public void testDownstreamTupleSender3 ()
    {
        final DownstreamTupleSender3 tupleSender = new DownstreamTupleSender3( 1, 2, 3, 4, 5, 6, tupleQueueContext );
        tuples.add( 1, new Tuple( "key1", "val" ) );
        tuples.add( 3, new Tuple( "key2", "val" ) );
        tuples.add( 5, new Tuple( "key3", "val" ) );

        tupleSender.send( tuples );

        verify( tupleQueueContext ).offer( 2, singletonList( new Tuple( "key1", "val" ) ) );
        verify( tupleQueueContext ).offer( 4, singletonList( new Tuple( "key2", "val" ) ) );
        verify( tupleQueueContext ).offer( 6, singletonList( new Tuple( "key3", "val" ) ) );
    }

    @Test
    public void testDownstreamTupleSender4 ()
    {
        final DownstreamTupleSender4 tupleSender = new DownstreamTupleSender4( 1, 2, 3, 4, 5, 6, 7, 8, tupleQueueContext );
        tuples.add( 1, new Tuple( "key1", "val" ) );
        tuples.add( 3, new Tuple( "key2", "val" ) );
        tuples.add( 5, new Tuple( "key3", "val" ) );
        tuples.add( 7, new Tuple( "key4", "val" ) );

        tupleSender.send( tuples );

        verify( tupleQueueContext ).offer( 2, singletonList( new Tuple( "key1", "val" ) ) );
        verify( tupleQueueContext ).offer( 4, singletonList( new Tuple( "key2", "val" ) ) );
        verify( tupleQueueContext ).offer( 6, singletonList( new Tuple( "key3", "val" ) ) );
        verify( tupleQueueContext ).offer( 8, singletonList( new Tuple( "key4", "val" ) ) );
    }

    @Test
    public void testDownstreamTupleSenderN ()
    {
        final DownstreamTupleSenderN tupleSender = new DownstreamTupleSenderN( new int[] { 1, 3, 5, 7, 9 },
                                                                               new int[] { 2, 4, 6, 8, 10 },
                                                                               tupleQueueContext );
        tuples.add( 1, new Tuple( "key1", "val" ) );
        tuples.add( 3, new Tuple( "key2", "val" ) );
        tuples.add( 5, new Tuple( "key3", "val" ) );
        tuples.add( 7, new Tuple( "key4", "val" ) );
        tuples.add( 9, new Tuple( "key5", "val" ) );

        tupleSender.send( tuples );

        verify( tupleQueueContext ).offer( 2, singletonList( new Tuple( "key1", "val" ) ) );
        verify( tupleQueueContext ).offer( 4, singletonList( new Tuple( "key2", "val" ) ) );
        verify( tupleQueueContext ).offer( 6, singletonList( new Tuple( "key3", "val" ) ) );
        verify( tupleQueueContext ).offer( 8, singletonList( new Tuple( "key4", "val" ) ) );
        verify( tupleQueueContext ).offer( 10, singletonList( new Tuple( "key5", "val" ) ) );
    }

}
