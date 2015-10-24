package cs.bilkent.zanza.operator.scheduling;


import cs.bilkent.zanza.operator.SchedulingStrategy;

public class ScheduleWhenAvailable implements SchedulingStrategy
{
    public static final ScheduleWhenAvailable INSTANCE = new ScheduleWhenAvailable();

    private ScheduleWhenAvailable ()
    {

    }
}
