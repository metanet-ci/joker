package cs.bilkent.zanza.operator.impl.scheduling;

import java.util.concurrent.TimeUnit;

import cs.bilkent.zanza.operator.scheduling.SchedulingStrategy;

public class ScheduleDelayed implements SchedulingStrategy
{
	private final long delayAmount;

	private final TimeUnit timeUnit;

	public ScheduleDelayed(final long delayAmount, final TimeUnit timeUnit)
	{
		this.delayAmount = delayAmount;
		this.timeUnit = timeUnit;
	}

	public long getDelayAmount()
	{
		return delayAmount;
	}

	public TimeUnit getTimeUnit()
	{
		return timeUnit;
	}
}
