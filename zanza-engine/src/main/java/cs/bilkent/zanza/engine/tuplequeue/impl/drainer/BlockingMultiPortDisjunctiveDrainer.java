package cs.bilkent.zanza.engine.tuplequeue.impl.drainer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import cs.bilkent.zanza.engine.tuplequeue.TupleQueue;
import cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.PortToTupleCount;
import cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount;


public class BlockingMultiPortDisjunctiveDrainer extends MultiPortDrainer
{

    private int timeoutInMillis;

    public BlockingMultiPortDisjunctiveDrainer ( final TupleAvailabilityByCount tupleAvailabilityByCount,
                                                 final List<PortToTupleCount> tupleCountByPortIndex,
                                                 final int timeoutInMillis )
    {
        super( tupleAvailabilityByCount, tupleCountByPortIndex );
        this.timeoutInMillis = timeoutInMillis;
    }

    @Override
    protected List<PortToTupleCount> checkQueueSizes ( final TupleQueue[] tupleQueues )
    {
        final int timeoutInMillisPerQueue = (int) Math.ceil( timeoutInMillis / tupleQueues.length );
        List<PortToTupleCount> satisfied = null;

        for ( PortToTupleCount p : tupleCountByPortIndex )
        {
            final TupleQueue tupleQueue = tupleQueues[ p.portIndex ];
            if ( satisfied == null )
            {
                if ( tupleQueue.awaitMinimumSize( p.tupleCount, timeoutInMillisPerQueue ) )
                {
                    satisfied = new ArrayList<>( tupleCountByPortIndex.size() );
                    satisfied.add( p );
                }
            }
            else if ( tupleQueue.size() >= p.tupleCount )
            {
                satisfied.add( p );
            }
        }

        return satisfied != null ? satisfied : Collections.emptyList();
    }

}
