package cs.bilkent.joker.operator.scheduling;

/**
 * Specifies that an operator can be invoked at anytime by the runtime engine. It can be used for the operators with no input ports.
 */
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
