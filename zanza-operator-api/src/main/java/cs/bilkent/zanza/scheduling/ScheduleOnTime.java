package cs.bilkent.zanza.scheduling;


public class ScheduleOnTime implements SchedulingStrategy
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
