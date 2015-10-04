package cs.bilkent.zanza.operator.scheduling;


import java.util.concurrent.TimeUnit;

import cs.bilkent.zanza.operator.Port;
import cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityType;
import static cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityType.AVAILABLE_ON_ALL;
import static cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityType.AVAILABLE_ON_ANY;


public final class SchedulingStrategies
{

    public static final SchedulingStrategy SCHEDULE_WHEN_AVAILABLE = new ScheduleWhenAvailable();

    private SchedulingStrategies()
    {
    }

    public static SchedulingStrategy scheduleWhenSingleTupleAvailableOnDefaultPort()
    {
        return new ScheduleWhenTuplesAvailable();
    }

    public static SchedulingStrategy scheduleWhenTtuplesAvailableOnDefaultPort(final int tupleCount)
    {
        return new ScheduleWhenTuplesAvailable(Port.DEFAULT_PORT_INDEX, tupleCount);
    }

    public static SchedulingStrategy scheduleWhenTuplesAvailable(final TupleAvailabilityType type, final int tupleCount,
            final int... ports)
    {
        return new ScheduleWhenTuplesAvailable(type, tupleCount, ports);
    }

    public static SchedulingStrategy scheduleWhenTuplesAvailableOnAll(final int tupleCount, final int... ports)
    {
        return new ScheduleWhenTuplesAvailable(AVAILABLE_ON_ALL, tupleCount, ports);
    }

    public static SchedulingStrategy scheduleWhenTuplesAvailableOnAny(final int tupleCount, final int... ports)
    {
        return new ScheduleWhenTuplesAvailable(AVAILABLE_ON_ANY, tupleCount, ports);
    }

    public static SchedulingStrategy scheduleAfterDelay(final long delayAmount, final TimeUnit timeUnit)
    {
        return new ScheduleDelayed(delayAmount, timeUnit);
    }

    public static SchedulingStrategy scheduleOnTime(final long timestampInMillis)
    {
        return new ScheduleOnTime(timestampInMillis);
    }
}
