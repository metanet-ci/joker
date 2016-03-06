package cs.bilkent.zanza.engine.tuplequeue.impl.drainer;

import java.util.Collections;
import java.util.List;

import cs.bilkent.zanza.engine.tuplequeue.TupleQueue;
import cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.PortToTupleCount;
import cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount;

public class BlockingMultiPortConjunctiveDrainer extends MultiPortDrainer
{

    private int timeoutInMillis;

    public BlockingMultiPortConjunctiveDrainer ( final TupleAvailabilityByCount tupleAvailabilityByCount,
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

        int satisfied = 0;
        for ( PortToTupleCount p : tupleCountByPortIndex )
        {
            if ( tupleQueues[ p.portIndex ].awaitMinimumSize( p.tupleCount, timeoutInMillisPerQueue ) )
            {
                satisfied++;
            }
        }

        return satisfied == tupleCountByPortIndex.size() ? tupleCountByPortIndex : Collections.emptyList();
    }

}
