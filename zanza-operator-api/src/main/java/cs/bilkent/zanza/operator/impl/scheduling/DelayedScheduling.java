package cs.bilkent.zanza.operator.impl.scheduling;

import java.util.concurrent.TimeUnit;

import cs.bilkent.zanza.operator.scheduling.SchedulingStrategy;

public class DelayedScheduling implements SchedulingStrategy
{

	private final long delayAmount;

	private final TimeUnit timeUnit;

	public DelayedScheduling(long delayAmount, TimeUnit timeUnit)
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
