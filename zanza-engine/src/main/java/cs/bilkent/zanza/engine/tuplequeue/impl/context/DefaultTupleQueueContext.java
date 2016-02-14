package cs.bilkent.zanza.engine.tuplequeue.impl.context;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueue;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueContext;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueuesConsumer;
import cs.bilkent.zanza.operator.PortsToTuples;
import cs.bilkent.zanza.operator.PortsToTuples.PortToTuples;
import cs.bilkent.zanza.scheduling.ScheduleWhenTuplesAvailable.PortToTupleCount;


public class DefaultTupleQueueContext implements TupleQueueContext
{

    private static final Logger LOGGER = LoggerFactory.getLogger( DefaultTupleQueueContext.class );


    private final String operatorId;

    private final int inputPortCount;


    private final TupleQueue[] tupleQueues;


    public DefaultTupleQueueContext ( final String operatorId, final int inputPortCount, final Supplier<TupleQueue> tupleQueueSupplier )
    {
        this.operatorId = operatorId;
        this.inputPortCount = inputPortCount;
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
    public void add ( final PortsToTuples portsToTuples )
    {
        for ( PortToTuples portToTuples : portsToTuples.getPortToTuplesList() )
        {
            final int portIndex = portToTuples.getPortIndex();
            checkArgument( portIndex < this.inputPortCount,
                           "Tuples have invalid input port index for operator: " + operatorId + " input port count: " + inputPortCount
                           + " input port " + "index: " + portIndex );

            final TupleQueue tupleQueue = tupleQueues[ portIndex ];
            tupleQueue.offerTuples( portToTuples.getTuples() );
        }
    }

    @Override
    public List<PortToTupleCount> tryAdd ( final PortsToTuples portsToTuples, final long timeoutInMillis )
    {
        final List<PortToTuples> portToTuplesList = portsToTuples.getPortToTuplesList();

        if ( portToTuplesList.isEmpty() )
        {
            return Collections.emptyList();
        }

        final List<PortToTupleCount> counts = new ArrayList<>();
        for ( PortToTuples portToTuples : portToTuplesList )
        {
            final int portIndex = portToTuples.getPortIndex();
            checkArgument( portIndex < this.inputPortCount,
                           "Tuples have invalid input port index for operator: " + operatorId + " input port count: " + inputPortCount
                           + " input port " + "index: " + portIndex );

            final TupleQueue tupleQueue = tupleQueues[ portIndex ];
            final int count = tupleQueue.tryOfferTuples( portToTuples.getTuples(), timeoutInMillis );
            counts.add( new PortToTupleCount( portIndex, count ) );
        }

        return counts;
    }

    @Override
    public void drain ( TupleQueuesConsumer tupleQueuesConsumer )
    {
        tupleQueuesConsumer.accept( tupleQueues );
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
