package cs.bilkent.zanza.engine.tuplequeue.impl.context;

import java.util.List;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkState;
import cs.bilkent.zanza.engine.config.ThreadingPreference;
import static cs.bilkent.zanza.engine.config.ThreadingPreference.MULTI_THREADED;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueue;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueDrainer;
import cs.bilkent.zanza.operator.Tuple;


public class DefaultTupleQueueContext extends AbstractTupleQueueContext
{

    private static final Logger LOGGER = LoggerFactory.getLogger( DefaultTupleQueueContext.class );


    private final TupleQueue[] tupleQueues;

    private final ThreadingPreference threadingPreference;

    public DefaultTupleQueueContext ( final String operatorId,
                                      final int inputPortCount,
                                      final ThreadingPreference threadingPreference,
                                      final Function<Boolean, TupleQueue> tupleQueueConstructor

    )
    {
        super( operatorId, inputPortCount );
        this.threadingPreference = threadingPreference;
        this.tupleQueues = new TupleQueue[ inputPortCount ];
        for ( int portIndex = 0; portIndex < inputPortCount; portIndex++ )
        {
            this.tupleQueues[ portIndex ] = tupleQueueConstructor.apply( true );
        }
    }

    @Override
    public String getOperatorId ()
    {
        return operatorId;
    }

    @Override
    protected TupleQueue[] getTupleQueues ( final List<Tuple> tuples )
    {
        return ( tuples == null || tuples.isEmpty() ) ? null : tupleQueues;
    }

    @Override
    public void drain ( TupleQueueDrainer drainer )
    {
        drainer.drain( null, tupleQueues );
    }

    @Override
    public void ensureCapacity ( final int portIndex, final int capacity )
    {
        tupleQueues[ portIndex ].ensureCapacity( capacity );
    }

    @Override
    public void clear ()
    {
        LOGGER.info( "Clearing tuple queues of operator: {}", operatorId );

        for ( TupleQueue tupleQueue : tupleQueues )
        {
            tupleQueue.clear();
        }
    }

    @Override
    public void enableCapacityCheck ( final int portIndex )
    {
        checkState( threadingPreference == MULTI_THREADED );
        tupleQueues[ portIndex ].enableCapacityCheck();
    }

    @Override
    public void disableCapacityCheck ( final int portIndex )
    {
        checkState( threadingPreference == MULTI_THREADED );
        tupleQueues[ portIndex ].disableCapacityCheck();
    }

    @Override
    public boolean isCapacityCheckEnabled ( final int portIndex )
    {
        checkState( threadingPreference == MULTI_THREADED );
        return tupleQueues[ portIndex ].isCapacityCheckEnabled();
    }

    @Override
    public boolean isCapacityCheckDisabled ( final int portIndex )
    {
        checkState( threadingPreference == MULTI_THREADED );
        return !tupleQueues[ portIndex ].isCapacityCheckEnabled();
    }

}
