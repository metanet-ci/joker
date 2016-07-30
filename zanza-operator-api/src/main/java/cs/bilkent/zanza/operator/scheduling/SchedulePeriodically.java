package cs.bilkent.zanza.operator.scheduling;

// NOT IMPLEMENTED YET
public final class SchedulePeriodically implements SchedulingStrategy
{
    private final long timestampInMillis;

    public SchedulePeriodically ( final long timestampInMillis )
    {
        this.timestampInMillis = timestampInMillis;
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    public long getTimestampInMillis ()
    {
        return timestampInMillis;
    }
}
