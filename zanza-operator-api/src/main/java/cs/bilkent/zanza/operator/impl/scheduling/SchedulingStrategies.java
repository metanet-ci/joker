package cs.bilkent.zanza.operator.impl.scheduling;

import static cs.bilkent.zanza.operator.impl.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityType.AVAILABLE_ON_ALL;
import static cs.bilkent.zanza.operator.impl.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityType.AVAILABLE_ON_ANY;

import java.util.concurrent.TimeUnit;

import cs.bilkent.zanza.operator.Port;
import cs.bilkent.zanza.operator.impl.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityType;
import cs.bilkent.zanza.operator.scheduling.SchedulingStrategy;

public final class SchedulingStrategies
{

	private SchedulingStrategies()
	{

	}

	public static SchedulingStrategy scheduleWhenSingleTupleAvailableOnDefaultPort()
	{
		return new ScheduleWhenTuplesAvailable();
	}

	public static SchedulingStrategy scheduleWhenTtuplesAvailableOnDefaultPort(int tupleCount)
	{
		return new ScheduleWhenTuplesAvailable(Port.DEFAULT_PORT_INDEX, tupleCount);
	}

	public static SchedulingStrategy scheduleWhenTuplesAvailable(TupleAvailabilityType type, int tupleCount, int... ports)
	{
		return new ScheduleWhenTuplesAvailable(type, tupleCount, ports);
	}

	public static SchedulingStrategy scheduleWhenTuplesAvailableOnAll(int tupleCount, int... ports)
	{
		return new ScheduleWhenTuplesAvailable(AVAILABLE_ON_ALL, tupleCount, ports);
	}

	public static SchedulingStrategy scheduleWhenTuplesAvailableOnAny(int tupleCount, int... ports)
	{
		return new ScheduleWhenTuplesAvailable(AVAILABLE_ON_ANY, tupleCount, ports);
	}

	public static SchedulingStrategy scheduleAfterDelay(long delayAmount, TimeUnit timeUnit)
	{
		return new DelayedScheduling(delayAmount, timeUnit);
	}

	public static SchedulingStrategy scheduleOnTime(long timestampInMillis)
	{
		return new OnTimeScheduling(timestampInMillis);
	}

}
