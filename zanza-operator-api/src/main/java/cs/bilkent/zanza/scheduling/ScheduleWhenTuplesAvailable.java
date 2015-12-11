package cs.bilkent.zanza.scheduling;


import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import cs.bilkent.zanza.flow.Port;
import static cs.bilkent.zanza.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.AT_LEAST;
import static cs.bilkent.zanza.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.AT_LEAST_BUT_SAME_ON_ALL_PORTS;
import static cs.bilkent.zanza.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByPort.AVAILABLE_ON_ALL_PORTS;
import static cs.bilkent.zanza.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByPort.AVAILABLE_ON_ANY_PORT;
import static java.util.Collections.unmodifiableMap;

public class ScheduleWhenTuplesAvailable implements SchedulingStrategy
{

    public enum TupleAvailabilityByPort
    {
        AVAILABLE_ON_ALL_PORTS, AVAILABLE_ON_ANY_PORT
    }


    public enum TupleAvailabilityByCount
    {
        EXACT,
        AT_LEAST,
        AT_LEAST_BUT_SAME_ON_ALL_PORTS
    }


    public static final int ANY_NUMBER_OF_TUPLES = 0;

    private final Map<Integer, Integer> tupleCountByPortIndex = new HashMap<>();

    private final TupleAvailabilityByCount tupleAvailabilityByCount;

    private final TupleAvailabilityByPort tupleAvailabilityByPort;

    public static ScheduleWhenTuplesAvailable scheduleWhenTuplesAvailableOnAll ( final int tupleCount, final int... ports )
    {
        return new ScheduleWhenTuplesAvailable( AT_LEAST, AVAILABLE_ON_ALL_PORTS, tupleCount, ports );
    }

    public static ScheduleWhenTuplesAvailable scheduleWhenTuplesAvailableOnAll ( final TupleAvailabilityByCount tupleAvailabilityByCount,
                                                                                 final int tupleCount,
                                                                                 final int... ports )
    {
        return new ScheduleWhenTuplesAvailable( tupleAvailabilityByCount, AVAILABLE_ON_ALL_PORTS, tupleCount, ports );
    }

    public static ScheduleWhenTuplesAvailable scheduleWhenTuplesAvailableOnAll ( final int tupleCount, final List<Integer> ports )
    {
        return new ScheduleWhenTuplesAvailable( AT_LEAST, AVAILABLE_ON_ALL_PORTS, tupleCount, ports );
    }

    public static ScheduleWhenTuplesAvailable scheduleWhenTuplesAvailableOnAll ( final TupleAvailabilityByCount tupleAvailabilityByCount,
                                                                                 final int tupleCount,
                                                                                 final List<Integer> ports )
    {
        return new ScheduleWhenTuplesAvailable( tupleAvailabilityByCount, AVAILABLE_ON_ALL_PORTS, tupleCount, ports );
    }

    public static ScheduleWhenTuplesAvailable scheduleWhenTuplesAvailableOnAny ( final int tupleCount, final int... ports )
    {
        return new ScheduleWhenTuplesAvailable( AT_LEAST, AVAILABLE_ON_ANY_PORT, tupleCount, ports );
    }

    public static ScheduleWhenTuplesAvailable scheduleWhenTuplesAvailableOnAny ( final TupleAvailabilityByCount tupleAvailabilityByCount,
                                                                                 final int tupleCount,
                                                                                 final int... ports )
    {
        checkArgument( tupleAvailabilityByCount != AT_LEAST_BUT_SAME_ON_ALL_PORTS );
        return new ScheduleWhenTuplesAvailable( tupleAvailabilityByCount, AVAILABLE_ON_ANY_PORT, tupleCount, ports );
    }

    public static ScheduleWhenTuplesAvailable scheduleWhenTuplesAvailableOnAny ( final int tupleCount, final List<Integer> ports )
    {
        return new ScheduleWhenTuplesAvailable( AT_LEAST, AVAILABLE_ON_ANY_PORT, tupleCount, ports );
    }

    public static ScheduleWhenTuplesAvailable scheduleWhenTuplesAvailableOnAny ( final TupleAvailabilityByCount tupleAvailabilityByCount,
                                                                                 final int tupleCount,
                                                                                 final List<Integer> ports )
    {
        checkArgument( tupleAvailabilityByCount != AT_LEAST_BUT_SAME_ON_ALL_PORTS );
        return new ScheduleWhenTuplesAvailable( tupleAvailabilityByCount, AVAILABLE_ON_ANY_PORT, tupleCount, ports );
    }

    public static ScheduleWhenTuplesAvailable scheduleWhenTuplesAvailableOnDefaultPort ( final int tupleCount )
    {
        return new ScheduleWhenTuplesAvailable( Port.DEFAULT_PORT_INDEX, tupleCount );
    }

    public static ScheduleWhenTuplesAvailable scheduleWhenTuplesAvailableOnDefaultPort ( final TupleAvailabilityByCount tupleAvailabilityByCount,
                                                                                         final int tupleCount )
    {
        return new ScheduleWhenTuplesAvailable( tupleAvailabilityByCount, Port.DEFAULT_PORT_INDEX, tupleCount );
    }


    public ScheduleWhenTuplesAvailable ( final int portIndex, final int tupleCount )
    {
        this.tupleAvailabilityByCount = AT_LEAST;
        this.tupleAvailabilityByPort = AVAILABLE_ON_ANY_PORT;
        tupleCountByPortIndex.put( portIndex, tupleCount );
    }

    public ScheduleWhenTuplesAvailable ( final TupleAvailabilityByCount tupleAvailabilityByCount,
                                         final int portIndex,
                                         final int tupleCount )
    {
        this.tupleAvailabilityByCount = tupleAvailabilityByCount;
        this.tupleAvailabilityByPort = AVAILABLE_ON_ANY_PORT;
        tupleCountByPortIndex.put( portIndex, tupleCount );
    }

    public ScheduleWhenTuplesAvailable ( final Map<Integer, Integer> tupleCountByPortIndex )
    {
        this.tupleAvailabilityByCount = AT_LEAST;
        this.tupleAvailabilityByPort = AVAILABLE_ON_ALL_PORTS;
        this.tupleCountByPortIndex.putAll( tupleCountByPortIndex );
    }

    public ScheduleWhenTuplesAvailable ( final TupleAvailabilityByCount tupleAvailabilityByCount,
                                         final TupleAvailabilityByPort tupleAvailabilityByPort,
                                         final Map<Integer, Integer> tupleCountByPortIndex )
    {
        checkNotNull( tupleAvailabilityByCount );
        checkNotNull( tupleAvailabilityByPort );
        checkArgument( tupleCountByPortIndex.size() == 1 || !( tupleAvailabilityByCount == AT_LEAST_BUT_SAME_ON_ALL_PORTS
                                                               && tupleAvailabilityByPort == AVAILABLE_ON_ANY_PORT ) );
        this.tupleAvailabilityByCount = tupleAvailabilityByCount;
        this.tupleAvailabilityByPort = tupleAvailabilityByPort;
        this.tupleCountByPortIndex.putAll( tupleCountByPortIndex );
    }

    public ScheduleWhenTuplesAvailable ( final TupleAvailabilityByCount tupleAvailabilityByCount,
                                         final TupleAvailabilityByPort tupleAvailabilityByPort,
                                         final int tupleCount,
                                         final int... ports )
    {
        checkNotNull( tupleAvailabilityByCount );
        checkNotNull( tupleAvailabilityByPort );
        if ( ports.length == 0 )
        {
            throw new IllegalArgumentException();
        }

        this.tupleAvailabilityByCount = tupleAvailabilityByCount;
        this.tupleAvailabilityByPort = tupleAvailabilityByPort;
        for ( final int port : ports )
        {
            this.tupleCountByPortIndex.put( port, tupleCount );
        }
    }

    public ScheduleWhenTuplesAvailable ( final TupleAvailabilityByCount tupleAvailabilityByCount,
                                         final TupleAvailabilityByPort tupleAvailabilityByPort,
                                         final int tupleCount,
                                         final List<Integer> ports )
    {
        checkNotNull( tupleAvailabilityByCount );
        checkNotNull( tupleAvailabilityByPort );
        if ( ports.size() == 0 )
        {
            throw new IllegalArgumentException();
        }

        this.tupleAvailabilityByCount = tupleAvailabilityByCount;
        this.tupleAvailabilityByPort = tupleAvailabilityByPort;
        for ( final int port : ports )
        {
            this.tupleCountByPortIndex.put( port, tupleCount );
        }
    }

    public Map<Integer, Integer> getTupleCountByPortIndex ()
    {
        return unmodifiableMap( tupleCountByPortIndex );
    }

    public TupleAvailabilityByPort getTupleAvailabilityByPort ()
    {
        return tupleAvailabilityByPort;
    }

    public TupleAvailabilityByCount getTupleAvailabilityByCount ()
    {
        return tupleAvailabilityByCount;
    }

    public int getTupleCount ( final int portIndex )
    {
        return tupleCountByPortIndex.getOrDefault( portIndex, 0 );
    }

}
