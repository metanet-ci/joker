package cs.bilkent.zanza.operator.impl.scheduling;

import cs.bilkent.zanza.operator.scheduling.SchedulingStrategy;

public class OnTimeScheduling implements SchedulingStrategy
{

	private final long timestampInMillis;

	public OnTimeScheduling(long timestampInMillis)
	{
		this.timestampInMillis = timestampInMillis;
	}

	public long getTimestampInMillis()
	{
		return timestampInMillis;
	}

}
