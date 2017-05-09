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

public class SmartAdaptationTracker extends DefaultAdaptationTracker implements AdaptationTracker
{

    private static final Logger LOGGER = LoggerFactory.getLogger( SmartAdaptationTracker.class );


    private AdaptationConfig adaptationConfig;

    private ShutdownHook shutdownHook;

    private FlowExecutionPlan initialFlowExecutionPlan;

    private FlowMetrics initialFlowMetrics;

    private FlowExecutionPlan finalFlowExecutionPlan;

    private FlowMetrics finalFlowMetrics;

    private int stableCount;

    private volatile boolean shutdownTriggered;

    public SmartAdaptationTracker ( final JokerConfig config )
    {
        super( config );
        this.adaptationConfig = config.getAdaptationConfig();
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

        super.init( hook, initialFlowExecutionPlan );

        this.shutdownHook = hook;
        this.initialFlowExecutionPlan = initialFlowExecutionPlan;
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

        super.onPeriod( flowExecutionPlan, flowMetrics );

        if ( initialFlowMetrics == null )
        {
            checkState( stableCount == 0 );
            initialFlowMetrics = flowMetrics;
        }

        if ( stableCount++ > adaptationConfig.getStablePeriodCountToStop() )
        {
            finalFlowExecutionPlan = flowExecutionPlan;
            finalFlowMetrics = flowMetrics;
            LOGGER.info( "decided to shutdown with flow execution plan: " + finalFlowExecutionPlan.toPlanSummaryString() );
            shutdownHook.shutdown();
            shutdownTriggered = true;
        }
    }

    @Override
    public void onFlowExecutionPlanChange ( final FlowExecutionPlan newFlowExecutionPlan )
    {
        checkArgument( newFlowExecutionPlan != null );

        if ( !isEnabled() )
        {
            return;
        }

        checkRunning();

        super.onFlowExecutionPlanChange( newFlowExecutionPlan );

        stableCount = 0;
        LOGGER.info( "stable count is reset with new flow execution plan: " + newFlowExecutionPlan.toPlanSummaryString() );
    }

    public boolean isShutdownTriggered ()
    {
        return shutdownTriggered;
    }

    public FlowExecutionPlan getInitialFlowExecutionPlan ()
    {
        // happens-before
        checkState( shutdownTriggered );
        return initialFlowExecutionPlan;
    }

    public FlowMetrics getInitialFlowMetrics ()
    {
        // happens-before
        checkState( shutdownTriggered );
        return initialFlowMetrics;
    }

    public FlowExecutionPlan getFinalFlowExecutionPlan ()
    {
        // happens-before
        checkState( shutdownTriggered );
        return finalFlowExecutionPlan;
    }

    public FlowMetrics getFinalFlowMetrics ()
    {
        // happens-before
        checkState( shutdownTriggered );
        return finalFlowMetrics;
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

}
