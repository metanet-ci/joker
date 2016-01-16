package cs.bilkent.zanza.engine.tuplequeue.impl;

import org.junit.Test;

import cs.bilkent.zanza.engine.tuplequeue.TupleQueueContext;
import static cs.bilkent.zanza.engine.tuplequeue.TupleQueueManager.TupleQueueThreading.MULTI_THREADED;
import static cs.bilkent.zanza.engine.tuplequeue.TupleQueueManager.TupleQueueThreading.SINGLE_THREADED;
import cs.bilkent.zanza.engine.tuplequeue.impl.consumer.DrainAllAvailableTuples;
import cs.bilkent.zanza.operator.PortsToTuples;
import cs.bilkent.zanza.operator.Tuple;
import static cs.bilkent.zanza.operator.spec.OperatorType.PARTITIONED_STATEFUL;
import static cs.bilkent.zanza.operator.spec.OperatorType.STATEFUL;
import static cs.bilkent.zanza.operator.spec.OperatorType.STATELESS;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;


public class TupleQueueManagerImplTest
{

    private final TupleQueueManagerImpl tupleQueueManager = new TupleQueueManagerImpl();

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotCreateTupleQueueContexteWithoutOperatorId ()
    {
        tupleQueueManager.createTupleQueueContext( null, 1, STATEFUL, null, SINGLE_THREADED, 2 );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotCreateTupleQueueContextWithInvalidInputPortCount ()
    {
        tupleQueueManager.createTupleQueueContext( "op1", -1, STATEFUL, null, SINGLE_THREADED, 2 );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotCreateTupleQueueContextWithInvalidQueueCapacity ()
    {
        tupleQueueManager.createTupleQueueContext( "op1", 1, STATEFUL, null, SINGLE_THREADED, 0 );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotCreateTupleQueueContextWithoutOperatorType ()
    {
        tupleQueueManager.createTupleQueueContext( null, 1, null, null, SINGLE_THREADED, 1 );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotCreateTupleQueueContextWithoutTupleQueueType ()
    {
        tupleQueueManager.createTupleQueueContext( null, 1, STATELESS, null, null, 1 );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotCreateTupleQueueContextForPartitionedStatefulOperatorWithNullPartitionFieldNames ()
    {
        tupleQueueManager.createTupleQueueContext( "op1", 1, PARTITIONED_STATEFUL, null, MULTI_THREADED, 1 );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotCreateTupleQueueContextForPartitionedStatefulOperatorWithEmptyPartitionFieldNames ()
    {
        tupleQueueManager.createTupleQueueContext( "op1", 1, PARTITIONED_STATEFUL, emptyList(), MULTI_THREADED, 1 );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotCreateTupleQueueContextForStatefulOperatorWithPartitionFieldNames ()
    {
        tupleQueueManager.createTupleQueueContext( "op1", 1, STATEFUL, singletonList( "field1" ), MULTI_THREADED, 1 );
    }

    @Test
    public void shouldCreateTupleQueueContextForStatefulOperatorWithNullPartitionFieldNames ()
    {
        tupleQueueManager.createTupleQueueContext( "op1", 1, STATEFUL, null, MULTI_THREADED, 1 );
    }

    @Test
    public void shouldCreateTupleQueueContextForStatefulOperatorWithEmptyPartitionFieldNames ()
    {
        tupleQueueManager.createTupleQueueContext( "op1", 1, STATEFUL, emptyList(), MULTI_THREADED, 1 );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotCreateTupleQueueContextForStatelessOperatorWithPartitionFieldNames ()
    {
        tupleQueueManager.createTupleQueueContext( "op1", 1, STATELESS, singletonList( "field1" ), MULTI_THREADED, 1 );
    }

    @Test
    public void shouldCreateTupleQueueContextForStatelessOperatorWithNullPartitionFieldNames ()
    {
        tupleQueueManager.createTupleQueueContext( "op1", 1, STATELESS, null, MULTI_THREADED, 1 );
    }

    @Test
    public void shouldCreateTupleQueueContextForStatelessOperatorWithEmptyPartitionFieldNames ()
    {
        tupleQueueManager.createTupleQueueContext( "op1", 1, STATELESS, emptyList(), MULTI_THREADED, 1 );
    }

    @Test
    public void shouldCreateTupleQueueContextOnlyOnceForMultipleInvocations ()
    {
        final TupleQueueContext tupleQueueContext1 = tupleQueueManager.createTupleQueueContext( "op1",
                                                                                                1,
                                                                                                STATEFUL,
                                                                                                null,
                                                                                                MULTI_THREADED,
                                                                                                1 );

        final TupleQueueContext tupleQueueContext2 = tupleQueueManager.createTupleQueueContext( "op1",
                                                                                                1,
                                                                                                STATEFUL,
                                                                                                null,
                                                                                                MULTI_THREADED,
                                                                                                1 );

        assertTrue( tupleQueueContext1 == tupleQueueContext2 );
    }

    @Test
    public void shouldReleaseCleanTupleQueueContext ()
    {
        final TupleQueueContext tupleQueueContext = tupleQueueManager.createTupleQueueContext( "op1",
                                                                                               1,
                                                                                               STATEFUL,
                                                                                               null,
                                                                                               MULTI_THREADED,
                                                                                               1 );
        tupleQueueContext.add( new PortsToTuples( new Tuple() ) );
        tupleQueueManager.releaseTupleQueueContext( "op1" );
        final DrainAllAvailableTuples drainAllAvailableTuples = new DrainAllAvailableTuples();
        tupleQueueContext.drain( drainAllAvailableTuples );
        assertNull( drainAllAvailableTuples.getPortsToTuples() );
    }

    @Test
    public void shouldReCreateReleasedTupleQueueContext ()
    {
        final TupleQueueContext tupleQueueContex1 = tupleQueueManager.createTupleQueueContext( "op1",
                                                                                               1,
                                                                                               STATEFUL,
                                                                                               null,
                                                                                               MULTI_THREADED,
                                                                                               1 );
        tupleQueueManager.releaseTupleQueueContext( "op1" );
        final TupleQueueContext tupleQueueContex2 = tupleQueueManager.createTupleQueueContext( "op1",
                                                                                               1,
                                                                                               STATEFUL,
                                                                                               null,
                                                                                               MULTI_THREADED,
                                                                                               1 );
        assertFalse( tupleQueueContex1 == tupleQueueContex2 );
    }

}
