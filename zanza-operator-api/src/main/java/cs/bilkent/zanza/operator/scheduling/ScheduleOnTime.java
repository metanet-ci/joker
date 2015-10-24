package cs.bilkent.zanza.operator.scheduling;


import cs.bilkent.zanza.operator.SchedulingStrategy;

public class ScheduleOnTime implements SchedulingStrategy
{
    private final long timestampInMillis;

    public ScheduleOnTime(final long timestampInMillis)
    {
        this.timestampInMillis = timestampInMillis;
    }

    public long getTimestampInMillis()
    {
        return timestampInMillis;
    }
}
