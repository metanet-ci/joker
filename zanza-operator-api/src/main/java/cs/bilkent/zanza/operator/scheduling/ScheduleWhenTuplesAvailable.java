package cs.bilkent.zanza.operator.scheduling;


import java.util.HashMap;
import java.util.Map;

import cs.bilkent.zanza.operator.Port;
import static cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityType.AVAILABLE_ON_ALL;
import static cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityType.AVAILABLE_ON_ANY;

public class ScheduleWhenTuplesAvailable implements SchedulingStrategy
{

    public static final int ANY_NUMBER_OF_TUPLES = 0;

    private final Map<Integer, Integer> tupleCountByPortIndex = new HashMap<>();

    private final TupleAvailabilityType tupleAvailabilityType;

    public static ScheduleWhenTuplesAvailable scheduleWhenTuplesAvailableOnAll ( final int tupleCount, final int... ports )
    {
        return new ScheduleWhenTuplesAvailable( AVAILABLE_ON_ALL, tupleCount, ports );
    }

    public static ScheduleWhenTuplesAvailable scheduleWhenTuplesAvailableOnAny ( final int tupleCount, final int... ports )
    {
        return new ScheduleWhenTuplesAvailable( AVAILABLE_ON_ANY, tupleCount, ports );
    }

    public static ScheduleWhenTuplesAvailable scheduleWhenTuplesAvailableOnDefaultPort ( final int tupleCount )
    {
        return new ScheduleWhenTuplesAvailable( Port.DEFAULT_PORT_INDEX, tupleCount );
    }


    public ScheduleWhenTuplesAvailable ( final int portIndex, final int tupleCount )
    {
        this.tupleAvailabilityType = TupleAvailabilityType.AVAILABLE_ON_ANY;
        tupleCountByPortIndex.put( portIndex, tupleCount );
    }

    public ScheduleWhenTuplesAvailable ( final Map<Integer, Integer> tupleCountByPortIndex )
    {
        this.tupleAvailabilityType = TupleAvailabilityType.AVAILABLE_ON_ALL;
        this.tupleCountByPortIndex.putAll( tupleCountByPortIndex );
    }

    public ScheduleWhenTuplesAvailable ( final Map<Integer, Integer> tupleCountByPortIndex,
                                         final TupleAvailabilityType tupleAvailabilityType )
    {
        this.tupleAvailabilityType = tupleAvailabilityType;
        this.tupleCountByPortIndex.putAll( tupleCountByPortIndex );
    }

    public ScheduleWhenTuplesAvailable ( final TupleAvailabilityType type, final int tupleCount, final int... ports )
    {
        if ( ports.length == 0 )
        {
            throw new IllegalArgumentException();
        }

        for ( final int port : ports )
        {
            this.tupleCountByPortIndex.put( port, tupleCount );
        }

        this.tupleAvailabilityType = type;
    }

    public Map<Integer, Integer> getTupleCountByPortIndex ()
    {
        return tupleCountByPortIndex;
    }

    public TupleAvailabilityType getTupleAvailabilityType ()
    {
        return tupleAvailabilityType;
    }

    enum TupleAvailabilityType
    {
        AVAILABLE_ON_ALL, AVAILABLE_ON_ANY
    }
}
