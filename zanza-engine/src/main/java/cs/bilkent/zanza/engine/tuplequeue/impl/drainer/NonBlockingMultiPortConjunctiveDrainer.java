package cs.bilkent.zanza.engine.tuplequeue.impl.drainer;

import java.util.Collections;
import java.util.List;

import cs.bilkent.zanza.engine.tuplequeue.TupleQueue;
import cs.bilkent.zanza.scheduling.ScheduleWhenTuplesAvailable.PortToTupleCount;
import cs.bilkent.zanza.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount;


public class NonBlockingMultiPortConjunctiveDrainer extends MultiPortDrainer
{

    public NonBlockingMultiPortConjunctiveDrainer ( final TupleAvailabilityByCount tupleAvailabilityByCount,
                                                    final List<PortToTupleCount> tupleCountByPortIndex )
    {
        super( tupleAvailabilityByCount, tupleCountByPortIndex );
    }

    protected List<PortToTupleCount> checkQueueSizes ( final TupleQueue[] tupleQueues )
    {
        int satisfied = 0;
        for ( PortToTupleCount p : tupleCountByPortIndex )
        {
            if ( tupleQueues[ p.portIndex ].size() >= p.tupleCount )
            {
                satisfied++;
            }
        }

        return tupleCountByPortIndex.size() == satisfied ? tupleCountByPortIndex : Collections.emptyList();
    }

}
