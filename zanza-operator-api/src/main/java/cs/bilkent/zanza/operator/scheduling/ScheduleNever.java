package cs.bilkent.zanza.operator.scheduling;


public final class ScheduleNever implements SchedulingStrategy
{

    public final static ScheduleNever INSTANCE = new ScheduleNever();

    private ScheduleNever ()
    {

    }

    @Override
    public String toString ()
    {
        return "ScheduleNever{}";
    }

}
