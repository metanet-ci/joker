package cs.bilkent.joker.engine.adaptation.impl.adaptationtracker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cs.bilkent.joker.engine.adaptation.AdaptationTracker;
import cs.bilkent.joker.engine.config.AdaptationConfig;
import cs.bilkent.joker.engine.config.JokerConfig;
import cs.bilkent.joker.engine.flow.FlowExecPlan;
import cs.bilkent.joker.engine.metric.FlowMetrics;
import static cs.bilkent.joker.impl.com.google.common.base.Preconditions.checkArgument;
import static cs.bilkent.joker.impl.com.google.common.base.Preconditions.checkState;

public class ExperimentalAdaptationTracker implements AdaptationTracker
{

    private static final Logger LOGGER = LoggerFactory.getLogger( ExperimentalAdaptationTracker.class );


    private final AdaptationConfig adaptationConfig;

    private final FlowMetricsReporter flowMetricsReporter;

    private ShutdownHook shutdownHook;

    private FlowExecPlan execPlan;

    private FlowMetrics metrics;

    private int stableCount;

    private volatile boolean shutdownTriggered;

    public ExperimentalAdaptationTracker ( final JokerConfig jokerConfig, final FlowMetricsReporter flowMetricsReporter )
    {
        this.adaptationConfig = jokerConfig.getAdaptationConfig();
        this.flowMetricsReporter = flowMetricsReporter;
    }

    @Override
    public void init ( final ShutdownHook hook, final FlowExecPlan execPlan )
    {
        checkArgument( hook != null );
        checkArgument( execPlan != null );

        if ( !isEnabled() )
        {
            return;
        }

        checkNotInitialized();

        this.shutdownHook = hook;
        this.execPlan = execPlan;
    }

    @Override
    public void onPeriod ( final FlowExecPlan execPlan, final FlowMetrics metrics )
    {
        checkArgument( execPlan != null );
        checkArgument( metrics != null );

        if ( !isEnabled() )
        {
            return;
        }

        checkRunning();

        checkState( this.execPlan.equals( execPlan ) );

        this.metrics = metrics;

        if ( stableCount++ > adaptationConfig.getStablePeriodCountToStop() )
        {
            LOGGER.info( "Decided to shutdown with flow execution plan: " + execPlan.toSummaryString() );
            flowMetricsReporter.report( execPlan, metrics );
            shutdownHook.shutdown();
            shutdownTriggered = true;
        }
    }

    @Override
    public void onExecPlanChange ( final FlowExecPlan newExecPlan )
    {
        checkArgument( newExecPlan != null );
        checkArgument( !execPlan.equals( newExecPlan ) );

        if ( !isEnabled() )
        {
            return;
        }

        checkRunning();

        flowMetricsReporter.report( execPlan, metrics );
        this.execPlan = newExecPlan;
        this.metrics = null;

        stableCount = 0;
        LOGGER.info( "Stable period count is reset with new flow execution plan: " + newExecPlan.toSummaryString() );
    }

    public boolean isShutdownTriggered ()
    {
        return shutdownTriggered;
    }

    private void checkNotInitialized ()
    {
        checkState( this.shutdownHook == null );
    }

    private void checkRunning ()
    {
        checkState( this.shutdownHook != null );
        checkState( !shutdownTriggered );
    }

    private boolean isEnabled ()
    {
        return adaptationConfig.isAdaptationEnabled();
    }

    @FunctionalInterface
    public interface FlowMetricsReporter
    {
        void report ( FlowExecPlan execPlan, FlowMetrics metrics );
    }

}
