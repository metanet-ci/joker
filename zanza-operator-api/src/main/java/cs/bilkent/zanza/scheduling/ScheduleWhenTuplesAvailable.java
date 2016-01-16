package cs.bilkent.zanza.scheduling;


import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import cs.bilkent.zanza.flow.Port;
import static cs.bilkent.zanza.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.AT_LEAST;
import static cs.bilkent.zanza.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.AT_LEAST_BUT_SAME_ON_ALL_PORTS;
import static cs.bilkent.zanza.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByPort.AVAILABLE_ON_ALL_PORTS;
import static cs.bilkent.zanza.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByPort.AVAILABLE_ON_ANY_PORT;
import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableList;


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


    public static class PortToTupleCount
    {

        public final int portIndex;

        public final int tupleCount;

        public PortToTupleCount ( final int portIndex, final int tupleCount )
        {
            this.portIndex = portIndex;
            this.tupleCount = tupleCount;
        }

    }


    public static final int ANY_NUMBER_OF_TUPLES = 0;


    private final List<PortToTupleCount> tupleCountByPortIndex;

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

    public static ScheduleWhenTuplesAvailable scheduleWhenTuplesAvailableOnDefaultPort ( final TupleAvailabilityByCount
                                                                                                 tupleAvailabilityByCount,
                                                                                         final int tupleCount )
    {
        return new ScheduleWhenTuplesAvailable( tupleAvailabilityByCount, Port.DEFAULT_PORT_INDEX, tupleCount );
    }


    public ScheduleWhenTuplesAvailable ( final int portIndex, final int tupleCount )
    {
        this( AT_LEAST, AVAILABLE_ON_ANY_PORT, singletonList( new PortToTupleCount( portIndex, tupleCount ) ) );
    }

    public ScheduleWhenTuplesAvailable ( final TupleAvailabilityByCount tupleAvailabilityByCount,
                                         final int portIndex,
                                         final int tupleCount )
    {
        this( tupleAvailabilityByCount, AVAILABLE_ON_ANY_PORT, singletonList( new PortToTupleCount( portIndex, tupleCount ) ) );
    }

    public ScheduleWhenTuplesAvailable ( final List<PortToTupleCount> tupleCountByPortIndex )
    {
        this( AT_LEAST, AVAILABLE_ON_ALL_PORTS, tupleCountByPortIndex );
    }

    public ScheduleWhenTuplesAvailable ( final TupleAvailabilityByCount tupleAvailabilityByCount,
                                         final TupleAvailabilityByPort tupleAvailabilityByPort,
                                         final List<PortToTupleCount> tupleCountByPortIndex )
    {
        checkNotNull( tupleAvailabilityByCount );
        checkNotNull( tupleAvailabilityByPort );
        checkArgument( tupleCountByPortIndex.size() == 1 || !( tupleAvailabilityByCount == AT_LEAST_BUT_SAME_ON_ALL_PORTS
                                                               && tupleAvailabilityByPort == AVAILABLE_ON_ANY_PORT ) );
        this.tupleAvailabilityByCount = tupleAvailabilityByCount;
        this.tupleAvailabilityByPort = tupleAvailabilityByPort;
        final ArrayList<PortToTupleCount> copy = new ArrayList<>( tupleCountByPortIndex.size() );
        copy.addAll( tupleCountByPortIndex );
        this.tupleCountByPortIndex = unmodifiableList( copy );
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
        final ArrayList<PortToTupleCount> copy = new ArrayList<>( ports.length );
        for ( final int port : ports )
        {
            copy.add( new PortToTupleCount( port, tupleCount ) );
        }
        this.tupleCountByPortIndex = unmodifiableList( copy );
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
        final ArrayList<PortToTupleCount> copy = new ArrayList<>( ports.size() );
        for ( final int port : ports )
        {
            copy.add( new PortToTupleCount( port, tupleCount ) );
        }
        this.tupleCountByPortIndex = unmodifiableList( copy );
    }

    public List<PortToTupleCount> getTupleCountByPortIndex ()
    {
        return tupleCountByPortIndex;
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
        for ( PortToTupleCount p : tupleCountByPortIndex )
        {
            if ( p.portIndex == portIndex )
            {
                return p.tupleCount;
            }
        }

        return 0;
    }

}
