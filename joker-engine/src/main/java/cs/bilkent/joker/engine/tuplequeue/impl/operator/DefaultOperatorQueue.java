package cs.bilkent.joker.engine.tuplequeue.impl.operator;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.joker.engine.config.ThreadingPref;
import cs.bilkent.joker.engine.tuplequeue.OperatorQueue;
import cs.bilkent.joker.engine.tuplequeue.TupleQueue;
import cs.bilkent.joker.engine.tuplequeue.TupleQueueDrainer;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.impl.TuplesImpl;
import cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByPort;
import cs.bilkent.joker.partition.impl.PartitionKey;


public class DefaultOperatorQueue implements OperatorQueue
{

    private static final Logger LOGGER = LoggerFactory.getLogger( DefaultOperatorQueue.class );

    private final String operatorId;

    private final TupleQueue[] tupleQueues;

    private final ThreadingPref threadingPref;

    public DefaultOperatorQueue ( final String operatorId,
                                  final int inputPortCount,
                                  final ThreadingPref threadingPref, final TupleQueue[] tupleQueues )
    {
        checkArgument( inputPortCount >= 0 );
        checkArgument( threadingPref != null );
        checkArgument( tupleQueues != null );
        checkArgument( inputPortCount == tupleQueues.length );
        this.operatorId = operatorId;
        this.threadingPref = threadingPref;
        this.tupleQueues = Arrays.copyOf( tupleQueues, inputPortCount );
    }

    @Override
    public String getOperatorId ()
    {
        return operatorId;
    }

    @Override
    public int getInputPortCount ()
    {
        return tupleQueues.length;
    }

    @Override
    public boolean offer ( final int portIndex, final Tuple tuple )
    {
        return tupleQueues[ portIndex ].offer( tuple );
    }

    @Override
    public int offer ( final int portIndex, final List<Tuple> tuples )
    {
        return offer( portIndex, tuples, 0 );
    }

    @Override
    public int offer ( final int portIndex, final List<Tuple> tuples, final int fromIndex )
    {
        final TupleQueue[] tupleQueues = getTupleQueues( tuples );

        return tupleQueues != null ? tupleQueues[ portIndex ].offer( tuples, fromIndex ) : 0;
    }

    private TupleQueue[] getTupleQueues ( final List<Tuple> tuples )
    {
        return ( tuples == null || tuples.isEmpty() ) ? null : tupleQueues;
    }

    @Override
    public void drain ( final TupleQueueDrainer drainer, final Function<PartitionKey, TuplesImpl> tuplesSupplier )
    {
        drainer.drain( null, tupleQueues, tuplesSupplier );
    }

    @Override
    public void clear ()
    {
        LOGGER.debug( "Clearing tuple queues of operator: {}", operatorId );

        for ( int portIndex = 0; portIndex < getInputPortCount(); portIndex++ )
        {
            final TupleQueue tupleQueue = tupleQueues[ portIndex ];
            final int size = tupleQueue.size();
            if ( size > 1 )
            {
                if ( LOGGER.isDebugEnabled() )
                {
                    final List<Tuple> tuples = tupleQueue.poll( Integer.MAX_VALUE );
                    LOGGER.warn( "Tuple queue {} of operator: {} has {} tuples before clear: {}", portIndex, operatorId, size, tuples );
                }
                else
                {
                    LOGGER.warn( "Tuple queue {} of operator: {} has {} tuples before clear", portIndex, operatorId, size );
                }
            }
            tupleQueue.clear();
        }
    }

    @Override
    public void setTupleCounts ( final int[] tupleCounts, final TupleAvailabilityByPort tupleAvailabilityByPort )
    {
    }

    @Override
    public boolean isEmpty ()
    {
        for ( int portIndex = 0; portIndex < getInputPortCount(); portIndex++ )
        {
            if ( tupleQueues[ portIndex ].size() > 0 )
            {
                return false;
            }
        }

        return true;
    }

    @Override
    public void ensureCapacity ( final int capacity )
    {
        for ( int portIndex = 0; portIndex < getInputPortCount(); portIndex++ )
        {
            if ( tupleQueues[ portIndex ].ensureCapacity( capacity ) )
            {
                LOGGER.debug( "tuple queue of port index {} of operator {} is extended to {}", portIndex, operatorId, capacity );
            }
        }
    }

    public ThreadingPref getThreadingPref ()
    {
        return threadingPref;
    }

    public TupleQueue getTupleQueue ( final int portIndex )
    {
        return tupleQueues[ portIndex ];
    }

}
