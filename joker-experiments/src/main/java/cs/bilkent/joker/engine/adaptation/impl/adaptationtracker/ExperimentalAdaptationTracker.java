package cs.bilkent.joker.engine.adaptation.impl.adaptationtracker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cs.bilkent.joker.engine.adaptation.AdaptationTracker;
import cs.bilkent.joker.engine.config.AdaptationConfig;
import cs.bilkent.joker.engine.config.JokerConfig;
import cs.bilkent.joker.engine.flow.FlowExecutionPlan;
import cs.bilkent.joker.engine.metric.FlowMetrics;
import static cs.bilkent.joker.impl.com.google.common.base.Preconditions.checkArgument;
import static cs.bilkent.joker.impl.com.google.common.base.Preconditions.checkState;

public class ExperimentalAdaptationTracker implements AdaptationTracker
{

    private static final Logger LOGGER = LoggerFactory.getLogger( ExperimentalAdaptationTracker.class );


    private final AdaptationConfig adaptationConfig;

    private final FlowMetricsReporter flowMetricsReporter;

    private ShutdownHook shutdownHook;

    private FlowExecutionPlan flowExecutionPlan;

    private FlowMetrics flowMetrics;

    private int stableCount;

    private volatile boolean shutdownTriggered;

    public ExperimentalAdaptationTracker ( final JokerConfig jokerConfig, final FlowMetricsReporter flowMetricsReporter )
    {
        this.adaptationConfig = jokerConfig.getAdaptationConfig();
        this.flowMetricsReporter = flowMetricsReporter;
    }

    @Override
    public void init ( final ShutdownHook hook, final FlowExecutionPlan initialFlowExecutionPlan )
    {
        checkArgument( hook != null );
        checkArgument( initialFlowExecutionPlan != null );

        if ( !isEnabled() )
        {
            return;
        }

        checkNotInitialized();

        this.shutdownHook = hook;
        this.flowExecutionPlan = initialFlowExecutionPlan;
    }

    @Override
    public void onPeriod ( final FlowExecutionPlan flowExecutionPlan, final FlowMetrics flowMetrics )
    {
        checkArgument( flowExecutionPlan != null );
        checkArgument( flowMetrics != null );

        if ( !isEnabled() )
        {
            return;
        }

        checkRunning();

        checkState( this.flowExecutionPlan.equals( flowExecutionPlan ) );

        this.flowMetrics = flowMetrics;

        if ( stableCount++ > adaptationConfig.getStablePeriodCountToStop() )
        {
            LOGGER.info( "Decided to shutdown with flow execution plan: " + flowExecutionPlan.toPlanSummaryString() );
            flowMetricsReporter.report( flowExecutionPlan, flowMetrics );
            shutdownHook.shutdown();
            shutdownTriggered = true;
        }
    }

    @Override
    public void onFlowExecutionPlanChange ( final FlowExecutionPlan newFlowExecutionPlan )
    {
        checkArgument( newFlowExecutionPlan != null );
        checkArgument( !flowExecutionPlan.equals( newFlowExecutionPlan ) );

        if ( !isEnabled() )
        {
            return;
        }

        checkRunning();

        flowMetricsReporter.report( flowExecutionPlan, flowMetrics );
        this.flowExecutionPlan = newFlowExecutionPlan;
        this.flowMetrics = null;

        stableCount = 0;
        LOGGER.info( "Stable period count is reset with new flow execution plan: " + newFlowExecutionPlan.toPlanSummaryString() );
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
        void report ( FlowExecutionPlan flowExecutionPlan, FlowMetrics flowMetrics );
    }

}
