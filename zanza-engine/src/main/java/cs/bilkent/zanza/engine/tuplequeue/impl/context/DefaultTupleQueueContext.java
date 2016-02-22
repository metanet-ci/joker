package cs.bilkent.zanza.engine.tuplequeue.impl.context;

import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cs.bilkent.zanza.engine.tuplequeue.TupleQueue;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueDrainer;
import cs.bilkent.zanza.operator.PortsToTuples;


public class DefaultTupleQueueContext extends AbstractTupleQueueContext
{

    private static final Logger LOGGER = LoggerFactory.getLogger( DefaultTupleQueueContext.class );


    private final TupleQueue[] tupleQueues;

    public DefaultTupleQueueContext ( final String operatorId, final int inputPortCount, final Supplier<TupleQueue> tupleQueueSupplier )
    {
        super( operatorId, inputPortCount );
        this.tupleQueues = new TupleQueue[ inputPortCount ];
        for ( int portIndex = 0; portIndex < inputPortCount; portIndex++ )
        {
            this.tupleQueues[ portIndex ] = tupleQueueSupplier.get();
        }
    }

    @Override
    public String getOperatorId ()
    {
        return operatorId;
    }

    @Override
    protected TupleQueue[] getTupleQueues ( final PortsToTuples input )
    {
        return tupleQueues;
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

        for ( TupleQueue tupleQueue : tupleQueues )
        {
            tupleQueue.clear();
        }
    }

}
