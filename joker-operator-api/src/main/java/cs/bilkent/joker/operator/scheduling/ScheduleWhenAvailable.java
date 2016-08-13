package cs.bilkent.joker.operator.scheduling;


public final class ScheduleWhenAvailable implements SchedulingStrategy
{
    public static final ScheduleWhenAvailable INSTANCE = new ScheduleWhenAvailable();

    private ScheduleWhenAvailable ()
    {

    }

    @Override
    public String toString ()
    {
        return "ScheduleWhenAvailable{}";
    }

}
