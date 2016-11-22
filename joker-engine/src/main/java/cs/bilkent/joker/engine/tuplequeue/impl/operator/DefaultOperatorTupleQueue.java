package cs.bilkent.joker.engine.tuplequeue.impl.operator;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.joker.engine.config.ThreadingPreference;
import static cs.bilkent.joker.engine.config.ThreadingPreference.MULTI_THREADED;
import static cs.bilkent.joker.engine.config.ThreadingPreference.SINGLE_THREADED;
import cs.bilkent.joker.engine.tuplequeue.OperatorTupleQueue;
import cs.bilkent.joker.engine.tuplequeue.TupleQueue;
import cs.bilkent.joker.engine.tuplequeue.TupleQueueDrainer;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByPort;


public class DefaultOperatorTupleQueue implements OperatorTupleQueue
{

    private static final Logger LOGGER = LoggerFactory.getLogger( DefaultOperatorTupleQueue.class );

    private final String operatorId;

    private final TupleQueue[] tupleQueues;

    private final ThreadingPreference threadingPreference;

    private final int maxQueueSize;

    public DefaultOperatorTupleQueue ( final String operatorId,
                                       final int inputPortCount,
                                       final ThreadingPreference threadingPreference,
                                       final Function<Integer, TupleQueue> tupleQueueConstructor,
                                       final int maxQueueSize

    )
    {
        checkArgument( inputPortCount >= 0 );
        checkArgument( threadingPreference != null );
        checkArgument( tupleQueueConstructor != null );
        checkArgument( maxQueueSize > 0 );
        this.operatorId = operatorId;
        this.threadingPreference = threadingPreference;
        this.tupleQueues = new TupleQueue[ inputPortCount ];
        for ( int portIndex = 0; portIndex < inputPortCount; portIndex++ )
        {
            this.tupleQueues[ portIndex ] = tupleQueueConstructor.apply( portIndex );
        }
        this.maxQueueSize = threadingPreference == SINGLE_THREADED ? maxQueueSize : Integer.MAX_VALUE;
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
    public int offer ( final int portIndex, final List<Tuple> tuples )
    {
        return offer( portIndex, tuples, 0 );
    }

    @Override
    public int offer ( final int portIndex, final List<Tuple> tuples, final int fromIndex )
    {
        final TupleQueue[] tupleQueues = getTupleQueues( tuples );

        if ( tupleQueues != null )
        {
            return tupleQueues[ portIndex ].offerTuples( tuples, fromIndex );
        }

        return 0;
    }

    @Override
    public int offer ( final int portIndex, final List<Tuple> tuples, final long timeout, final TimeUnit unit )
    {
        return offer( portIndex, tuples, 0, timeout, unit );
    }

    @Override
    public int offer ( final int portIndex, final List<Tuple> tuples, final int fromIndex, final long timeout, final TimeUnit unit )
    {
        final TupleQueue[] tupleQueues = getTupleQueues( tuples );

        if ( tupleQueues != null )
        {
            return tupleQueues[ portIndex ].offerTuples( tuples, fromIndex, timeout, unit );
        }

        return 0;
    }

    private TupleQueue[] getTupleQueues ( final List<Tuple> tuples )
    {
        return ( tuples == null || tuples.isEmpty() ) ? null : tupleQueues;
    }

    @Override
    public void drain ( TupleQueueDrainer drainer )
    {
        drainer.drain( null, tupleQueues );
    }

    @Override
    public void clear ()
    {
        LOGGER.info( "Clearing tuple queues of operator: {}", operatorId );

        for ( int portIndex = 0; portIndex < tupleQueues.length; portIndex++ )
        {
            final TupleQueue tupleQueue = tupleQueues[ portIndex ];
            final int size = tupleQueue.size();
            if ( size > 1 )
            {
                if ( LOGGER.isDebugEnabled() )
                {
                    final List<Tuple> tuples = tupleQueue.pollTuplesAtLeast( 1 );
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
    public boolean isOverloaded ()
    {
        if ( threadingPreference == MULTI_THREADED )
        {
            return false;
        }

        for ( int i = 0; i < tupleQueues.length; i++ )
        {
            if ( tupleQueues[ i ].size() >= maxQueueSize )
            {
                return true;
            }
        }

        return false;
    }

    @Override
    public boolean isEmpty ()
    {
        for ( int i = 0; i < tupleQueues.length; i++ )
        {
            if ( tupleQueues[ i ].size() > 0 )
            {
                return false;
            }
        }

        return true;
    }

    public ThreadingPreference getThreadingPreference ()
    {
        return threadingPreference;
    }

    public TupleQueue getTupleQueue ( final int portIndex )
    {
        return tupleQueues[ portIndex ];
    }

}
