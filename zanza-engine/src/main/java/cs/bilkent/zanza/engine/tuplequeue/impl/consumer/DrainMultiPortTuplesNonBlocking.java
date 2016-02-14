package cs.bilkent.zanza.engine.tuplequeue.impl.consumer;

import cs.bilkent.zanza.engine.tuplequeue.TupleQueue;
import cs.bilkent.zanza.scheduling.ScheduleWhenTuplesAvailable;
import cs.bilkent.zanza.scheduling.ScheduleWhenTuplesAvailable.PortToTupleCount;
import static cs.bilkent.zanza.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByPort.AVAILABLE_ON_ALL_PORTS;


public class DrainMultiPortTuplesNonBlocking extends DrainMultiPortTuples
{

    public DrainMultiPortTuplesNonBlocking ( ScheduleWhenTuplesAvailable strategy )
    {
        super( strategy );
    }

    protected boolean checkQueueSizes ( final TupleQueue[] tupleQueues )
    {
        if ( tupleAvailabilityByPort == AVAILABLE_ON_ALL_PORTS )
        {
            for ( PortToTupleCount p : tupleCountByPortIndex )
            {
                if ( tupleQueues[ p.portIndex ].size() < p.tupleCount )
                {
                    return false;
                }
            }
        }

        return true;
    }

}
