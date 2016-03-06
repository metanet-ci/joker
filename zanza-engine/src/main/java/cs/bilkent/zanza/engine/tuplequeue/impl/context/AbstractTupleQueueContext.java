package cs.bilkent.zanza.engine.tuplequeue.impl.context;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueue;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueContext;
import cs.bilkent.zanza.operator.PortsToTuples;
import cs.bilkent.zanza.operator.PortsToTuples.PortToTuples;
import cs.bilkent.zanza.operator.Tuple;
import cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.PortToTupleCount;

public abstract class AbstractTupleQueueContext implements TupleQueueContext
{

    protected final String operatorId;

    protected final int inputPortCount;

    public AbstractTupleQueueContext ( final String operatorId, final int inputPortCount )
    {
        this.operatorId = operatorId;
        this.inputPortCount = inputPortCount;
    }

    protected abstract TupleQueue[] getTupleQueues ( final PortsToTuples input );

    @Override
    public void add ( final PortsToTuples input )
    {
        if ( input == null )
        {
            return;
        }

        final TupleQueue[] tupleQueues = getTupleQueues( input );

        if ( tupleQueues == null )
        {
            return;
        }

        for ( PortToTuples port : input.getPortToTuplesList() )
        {
            final int portIndex = port.getPortIndex();
            checkArgument( portIndex < this.inputPortCount,
                           "Tuples have invalid input port index for operator: " + operatorId + " input port count: " + inputPortCount
                           + " input port " + "index: " + portIndex );

            final TupleQueue tupleQueue = tupleQueues[ portIndex ];
            tupleQueue.offerTuples( port.getTuples() );
        }
    }

    @Override
    public List<PortToTupleCount> tryAdd ( final PortsToTuples input, final long timeoutInMillis )
    {
        final TupleQueue[] tupleQueues = getTupleQueues( input );

        if ( tupleQueues == null )
        {
            return Collections.emptyList();
        }

        List<PortToTupleCount> counts = null;
        for ( PortToTuples eachPort : input.getPortToTuplesList() )
        {
            final int portIndex = eachPort.getPortIndex();
            checkArgument( portIndex < this.inputPortCount,
                           "Tuples have invalid input port index for operator: " + operatorId + " input port count: " + inputPortCount
                           + " input port " + "index: " + portIndex );

            final TupleQueue tupleQueue = tupleQueues[ portIndex ];
            final List<Tuple> tuples = eachPort.getTuples();
            final int count = tupleQueue.tryOfferTuples( tuples, timeoutInMillis );
            if ( count < tuples.size() )
            {
                if ( counts == null )
                {
                    counts = new ArrayList<>( 1 );
                }
                counts.add( new PortToTupleCount( portIndex, count ) );
            }
        }

        return counts != null ? counts : Collections.emptyList();
    }

}
