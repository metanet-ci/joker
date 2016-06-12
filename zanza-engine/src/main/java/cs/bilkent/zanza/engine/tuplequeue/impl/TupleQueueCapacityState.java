package cs.bilkent.zanza.engine.tuplequeue.impl;

import java.util.concurrent.atomic.AtomicIntegerArray;

public class TupleQueueCapacityState
{

    private static final int CAPACITY_CHECK_ENABLED = 0, CAPACITY_CHECK_DISABLED = 1;

    private final AtomicIntegerArray capacityCheckFlags;

    public TupleQueueCapacityState ( final int portCount )
    {
        this.capacityCheckFlags = new AtomicIntegerArray( portCount );
    }

    public void enableCapacityCheck ( final int portIndex )
    {
        capacityCheckFlags.set( portIndex, CAPACITY_CHECK_ENABLED );
    }

    public void disableCapacityCheck ( final int portIndex )
    {
        capacityCheckFlags.set( portIndex, CAPACITY_CHECK_DISABLED );
    }

    public boolean isCapacityCheckEnabled ( final int portIndex )
    {
        return capacityCheckFlags.get( portIndex ) == CAPACITY_CHECK_ENABLED;
    }

    public boolean isCapacityCheckDisabled ( final int portIndex )
    {
        return capacityCheckFlags.get( portIndex ) == CAPACITY_CHECK_DISABLED;
    }

}
