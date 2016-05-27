package cs.bilkent.zanza.operator.scheduling;


public final class ScheduleOnTime implements SchedulingStrategy
{
    private final long timestampInMillis;

    public ScheduleOnTime ( final long timestampInMillis )
    {
        this.timestampInMillis = timestampInMillis;
    }

    public long getTimestampInMillis ()
    {
        return timestampInMillis;
    }
}
