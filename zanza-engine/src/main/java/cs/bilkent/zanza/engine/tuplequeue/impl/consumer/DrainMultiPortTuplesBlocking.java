package cs.bilkent.zanza.engine.tuplequeue.impl.consumer;

import java.util.concurrent.TimeUnit;

import cs.bilkent.zanza.engine.tuplequeue.TupleQueue;
import cs.bilkent.zanza.scheduling.ScheduleWhenTuplesAvailable;
import cs.bilkent.zanza.scheduling.ScheduleWhenTuplesAvailable.PortToTupleCount;


public class DrainMultiPortTuplesBlocking extends DrainMultiPortTuples
{

    private long timeoutInNanos;

    public DrainMultiPortTuplesBlocking ( ScheduleWhenTuplesAvailable strategy, final long timeoutInMillis )
    {
        super( strategy );
        this.timeoutInNanos = TimeUnit.MILLISECONDS.toNanos( timeoutInMillis );
    }

    protected boolean checkQueueSizes ( final TupleQueue[] tupleQueues )
    {
        final long startInNanos = System.nanoTime();
        boolean success;
        do
        {
            success = true;
            for ( PortToTupleCount p : tupleCountByPortIndex )
            {
                final int size = tupleQueues[ p.portIndex ].size();
                if ( size < p.tupleCount )
                {
                    success = false;
                    break;
                }
            }

            timeoutInNanos -= ( System.nanoTime() - startInNanos );
        } while ( timeoutInNanos > 0 && !success );

        return success;
    }

}
