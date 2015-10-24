package cs.bilkent.zanza.operator.scheduling;

import cs.bilkent.zanza.operator.SchedulingStrategy;

public class ScheduleNever implements SchedulingStrategy
{

    public final static ScheduleNever INSTANCE = new ScheduleNever();

    private ScheduleNever ()
    {

    }
}
