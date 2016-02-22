package cs.bilkent.zanza.engine.tuplequeue.impl.drainer;

import java.util.List;

import cs.bilkent.zanza.engine.tuplequeue.TupleQueue;
import cs.bilkent.zanza.scheduling.ScheduleWhenTuplesAvailable.PortToTupleCount;
import cs.bilkent.zanza.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount;


public class NonBlockingMultiPortDisjunctiveDrainer extends MultiPortDrainer
{

    public NonBlockingMultiPortDisjunctiveDrainer ( final TupleAvailabilityByCount tupleAvailabilityByCount,
                                                    final List<PortToTupleCount> tupleCountByPortIndex )
    {
        super( tupleAvailabilityByCount, tupleCountByPortIndex );
    }

    protected List<PortToTupleCount> checkQueueSizes ( final TupleQueue[] tupleQueues )
    {
        return tupleCountByPortIndex;
    }

}
