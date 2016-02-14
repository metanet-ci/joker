package cs.bilkent.zanza.engine.tuplequeue.impl;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import cs.bilkent.zanza.engine.tuplequeue.TupleQueueContext;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueManager.TupleQueueThreading;
import static cs.bilkent.zanza.engine.tuplequeue.TupleQueueManager.TupleQueueThreading.MULTI_THREADED;
import static cs.bilkent.zanza.engine.tuplequeue.TupleQueueManager.TupleQueueThreading.SINGLE_THREADED;
import cs.bilkent.zanza.engine.tuplequeue.impl.consumer.DrainAllAvailableTuples;
import cs.bilkent.zanza.operator.PortsToTuples;
import cs.bilkent.zanza.operator.Tuple;
import cs.bilkent.zanza.operator.spec.OperatorType;
import static cs.bilkent.zanza.operator.spec.OperatorType.PARTITIONED_STATEFUL;
import static cs.bilkent.zanza.operator.spec.OperatorType.STATEFUL;
import static cs.bilkent.zanza.operator.spec.OperatorType.STATELESS;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

@RunWith( Parameterized.class )
public class TupleQueueManagerImplReleaseTest
{

    @Parameters
    public static Collection<Object[]> data ()
    {
        return Arrays.asList( new Object[][] { { MULTI_THREADED, STATELESS, emptyList() },
                                               { MULTI_THREADED, PARTITIONED_STATEFUL, singletonList( "field1" ) },
                                               { MULTI_THREADED, STATEFUL, emptyList() },
                                               { SINGLE_THREADED, STATELESS, emptyList() },
                                               { SINGLE_THREADED, PARTITIONED_STATEFUL, singletonList( "field1" ) },
                                               { SINGLE_THREADED, STATEFUL, emptyList() } } );
    }


    private final TupleQueueManagerImpl tupleQueueManager = new TupleQueueManagerImpl();

    private final TupleQueueThreading threading;

    private final List<String> partitionFieldNames;

    private final OperatorType operatorType;

    public TupleQueueManagerImplReleaseTest ( final TupleQueueThreading threading,
                                              final OperatorType operatorType,
                                              final List<String> partitionFieldNames )
    {
        this.threading = threading;
        this.operatorType = operatorType;
        this.partitionFieldNames = partitionFieldNames;
    }

    @Test
    public void shouldReleaseCleanTupleQueueContext ()
    {
        final TupleQueueContext tupleQueueContext = tupleQueueManager.createTupleQueueContext( "op1",
                                                                                               1,
                                                                                               operatorType,
                                                                                               partitionFieldNames,
                                                                                               threading,
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
                                                                                               operatorType,
                                                                                               partitionFieldNames,
                                                                                               threading,
                                                                                               1 );
        tupleQueueManager.releaseTupleQueueContext( "op1" );
        final TupleQueueContext tupleQueueContex2 = tupleQueueManager.createTupleQueueContext( "op1",
                                                                                               1,
                                                                                               operatorType,
                                                                                               partitionFieldNames,
                                                                                               threading,
                                                                                               1 );
        assertFalse( tupleQueueContex1 == tupleQueueContex2 );
    }

}
