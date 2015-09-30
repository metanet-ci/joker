package cs.bilkent.zanza.operator.scheduling;

import java.util.concurrent.TimeUnit;

public class TimedSchedulingStrategy
        implements SchedulingStrategy {

    private final long initialDelay;

    private final TimeUnit initialDelayTimeUnit;

    private final long delay;

    private final TimeUnit delayTimeUnit;

    public TimedSchedulingStrategy(long delay, TimeUnit delayTimeUnit) {
        this(0, TimeUnit.MILLISECONDS, delay, delayTimeUnit);
    }

    public TimedSchedulingStrategy(long initialDelay, TimeUnit initialDelayTimeUnit, long delay, TimeUnit delayTimeUnit) {
        this.initialDelay = initialDelay;
        this.initialDelayTimeUnit = initialDelayTimeUnit;
        this.delay = delay;
        this.delayTimeUnit = delayTimeUnit;
    }

    public long getInitialDelay() {
        return initialDelay;
    }

    public TimeUnit getInitialDelayTimeUnit() {
        return initialDelayTimeUnit;
    }

    public long getDelay() {
        return delay;
    }

    public TimeUnit getDelayTimeUnit() {
        return delayTimeUnit;
    }

}
