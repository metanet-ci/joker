package cs.bilkent.zanza.operator.scheduling;


import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static cs.bilkent.zanza.flow.Port.DEFAULT_PORT_INDEX;
import static cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.AT_LEAST;
import static cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.AT_LEAST_BUT_SAME_ON_ALL_PORTS;
import static cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByPort.AVAILABLE_ON_ALL_PORTS;
import static cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByPort.AVAILABLE_ON_ANY_PORT;


public final class ScheduleWhenTuplesAvailable implements SchedulingStrategy
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


    public static ScheduleWhenTuplesAvailable scheduleWhenTuplesAvailableOnAll ( final int portCount,
                                                                                 final int tupleCount,
                                                                                 final int... ports )
    {
        return new ScheduleWhenTuplesAvailable( AT_LEAST, AVAILABLE_ON_ALL_PORTS, portCount, tupleCount, ports );
    }

    public static ScheduleWhenTuplesAvailable scheduleWhenTuplesAvailableOnAll ( final TupleAvailabilityByCount tupleAvailabilityByCount,
                                                                                 final int portCount,
                                                                                 final int tupleCount,
                                                                                 final int... ports )
    {
        return new ScheduleWhenTuplesAvailable( tupleAvailabilityByCount, AVAILABLE_ON_ALL_PORTS, portCount, tupleCount, ports );
    }

    public static ScheduleWhenTuplesAvailable scheduleWhenTuplesAvailableOnAll ( final int portCount,
                                                                                 final int tupleCount,
                                                                                 final List<Integer> ports )
    {
        return new ScheduleWhenTuplesAvailable( AT_LEAST, AVAILABLE_ON_ALL_PORTS, portCount, tupleCount, ports );
    }

    public static ScheduleWhenTuplesAvailable scheduleWhenTuplesAvailableOnAll ( final TupleAvailabilityByCount tupleAvailabilityByCount,
                                                                                 final int portCount,
                                                                                 final int tupleCount,
                                                                                 final List<Integer> ports )
    {
        return new ScheduleWhenTuplesAvailable( tupleAvailabilityByCount, AVAILABLE_ON_ALL_PORTS, portCount, tupleCount, ports );
    }

    public static ScheduleWhenTuplesAvailable scheduleWhenTuplesAvailableOnAny ( final int portCount,
                                                                                 final int tupleCount,
                                                                                 final int... ports )
    {
        return new ScheduleWhenTuplesAvailable( AT_LEAST, AVAILABLE_ON_ANY_PORT, portCount, tupleCount, ports );
    }

    public static ScheduleWhenTuplesAvailable scheduleWhenTuplesAvailableOnAny ( final TupleAvailabilityByCount tupleAvailabilityByCount,
                                                                                 final int portCount,
                                                                                 final int tupleCount,
                                                                                 final int... ports )
    {
        checkArgument( tupleAvailabilityByCount != AT_LEAST_BUT_SAME_ON_ALL_PORTS );
        return new ScheduleWhenTuplesAvailable( tupleAvailabilityByCount, AVAILABLE_ON_ANY_PORT, portCount, tupleCount, ports );
    }

    public static ScheduleWhenTuplesAvailable scheduleWhenTuplesAvailableOnAny ( final int portCount,
                                                                                 final int tupleCount,
                                                                                 final List<Integer> ports )
    {
        return new ScheduleWhenTuplesAvailable( AT_LEAST, AVAILABLE_ON_ANY_PORT, portCount, tupleCount, ports );
    }

    public static ScheduleWhenTuplesAvailable scheduleWhenTuplesAvailableOnAny ( final TupleAvailabilityByCount tupleAvailabilityByCount,
                                                                                 final int portCount,
                                                                                 final int tupleCount,
                                                                                 final List<Integer> ports )
    {
        checkArgument( tupleAvailabilityByCount != AT_LEAST_BUT_SAME_ON_ALL_PORTS );
        return new ScheduleWhenTuplesAvailable( tupleAvailabilityByCount, AVAILABLE_ON_ANY_PORT, portCount, tupleCount, ports );
    }

    public static ScheduleWhenTuplesAvailable scheduleWhenTuplesAvailableOnDefaultPort ( final int tupleCount )
    {
        return new ScheduleWhenTuplesAvailable( 1, DEFAULT_PORT_INDEX, tupleCount );
    }

    public static ScheduleWhenTuplesAvailable scheduleWhenTuplesAvailableOnDefaultPort ( final TupleAvailabilityByCount
                                                                                                 tupleAvailabilityByCount,
                                                                                         final int tupleCount )
    {
        return new ScheduleWhenTuplesAvailable( tupleAvailabilityByCount, 1, DEFAULT_PORT_INDEX, tupleCount );
    }


    private final int[] tupleCounts;

    private final TupleAvailabilityByCount tupleAvailabilityByCount;

    private final TupleAvailabilityByPort tupleAvailabilityByPort;

    public ScheduleWhenTuplesAvailable ( final int portCount, final int portIndex, final int tupleCount )
    {
        this( AT_LEAST, AVAILABLE_ON_ANY_PORT, portCount, tupleCount, portIndex );
    }

    public ScheduleWhenTuplesAvailable ( final TupleAvailabilityByCount tupleAvailabilityByCount, final int portCount,
                                         final int portIndex,
                                         final int tupleCount )
    {
        this( tupleAvailabilityByCount, AVAILABLE_ON_ANY_PORT, portCount, tupleCount, portIndex );
    }

    public ScheduleWhenTuplesAvailable ( final TupleAvailabilityByCount tupleAvailabilityByCount,
                                         final TupleAvailabilityByPort tupleAvailabilityByPort,
                                         final int[] tupleCounts )
    {
        checkNotNull( tupleAvailabilityByCount );
        checkNotNull( tupleAvailabilityByPort );
        checkArgument( tupleCounts.length == 1 || !( tupleAvailabilityByCount == AT_LEAST_BUT_SAME_ON_ALL_PORTS
                                                     && tupleAvailabilityByPort == AVAILABLE_ON_ANY_PORT ) );
        this.tupleAvailabilityByCount = tupleAvailabilityByCount;
        this.tupleAvailabilityByPort = tupleAvailabilityByPort;
        this.tupleCounts = tupleCounts;
    }

    public ScheduleWhenTuplesAvailable ( final TupleAvailabilityByCount tupleAvailabilityByCount,
                                         final TupleAvailabilityByPort tupleAvailabilityByPort,
                                         final int portCount,
                                         final int tupleCount,
                                         final int... ports )
    {
        checkNotNull( tupleAvailabilityByCount );
        checkNotNull( tupleAvailabilityByPort );
        checkArgument( portCount > 0 );
        checkArgument( ports != null && ports.length > 0 );

        this.tupleAvailabilityByCount = tupleAvailabilityByCount;
        this.tupleAvailabilityByPort = tupleAvailabilityByPort;
        this.tupleCounts = new int[ portCount ];
        for ( final int portIndex : ports )
        {
            tupleCounts[ portIndex ] = tupleCount;
        }
    }

    public ScheduleWhenTuplesAvailable ( final TupleAvailabilityByCount tupleAvailabilityByCount,
                                         final TupleAvailabilityByPort tupleAvailabilityByPort,
                                         final int portCount,
                                         final int tupleCount,
                                         final List<Integer> ports )
    {
        checkNotNull( tupleAvailabilityByCount );
        checkNotNull( tupleAvailabilityByPort );
        checkArgument( portCount > 0 );
        checkArgument( ports != null && ports.size() > 0 );

        this.tupleAvailabilityByCount = tupleAvailabilityByCount;
        this.tupleAvailabilityByPort = tupleAvailabilityByPort;
        this.tupleCounts = new int[ portCount ];
        for ( final int portIndex : ports )
        {
            tupleCounts[ portIndex ] = tupleCount;
        }
    }

    public int getPortCount ()
    {
        return tupleCounts.length;
    }

    public int[] getTupleCounts ()
    {
        return tupleCounts;
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
        return tupleCounts[ portIndex ];
    }

    @Override
    public boolean equals ( final Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        final ScheduleWhenTuplesAvailable that = (ScheduleWhenTuplesAvailable) o;

        if ( !Arrays.equals( tupleCounts, that.tupleCounts ) )
        {
            return false;
        }
        if ( tupleAvailabilityByCount != that.tupleAvailabilityByCount )
        {
            return false;
        }
        return tupleAvailabilityByPort == that.tupleAvailabilityByPort;

    }

    @Override
    public int hashCode ()
    {
        int result = Arrays.hashCode( tupleCounts );
        result = 31 * result + tupleAvailabilityByCount.hashCode();
        result = 31 * result + tupleAvailabilityByPort.hashCode();
        return result;
    }

    @Override
    public String toString ()
    {
        return "ScheduleWhenTuplesAvailable{" +
               "tupleCounts=" + Arrays.toString( tupleCounts ) +
               ", tupleAvailabilityByCount=" + tupleAvailabilityByCount +
               ", tupleAvailabilityByPort=" + tupleAvailabilityByPort +
               '}';
    }

}
