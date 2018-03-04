package cs.bilkent.joker.engine.adaptation.impl.adaptationtracker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cs.bilkent.joker.engine.adaptation.AdaptationTracker;
import cs.bilkent.joker.engine.config.JokerConfig;
import cs.bilkent.joker.engine.flow.FlowExecPlan;
import cs.bilkent.joker.engine.metric.FlowMetrics;
import static cs.bilkent.joker.impl.com.google.common.base.Preconditions.checkArgument;
import static cs.bilkent.joker.impl.com.google.common.base.Preconditions.checkState;

public class SmartAdaptationTracker extends DefaultAdaptationTracker implements AdaptationTracker
{

    private static final Logger LOGGER = LoggerFactory.getLogger( SmartAdaptationTracker.class );


    private ShutdownHook shutdownHook;

    private FlowExecPlan initialExecPlan;

    private FlowMetrics initialMetrics;

    private FlowExecPlan finalExecPlan;

    private FlowMetrics finalMetrics;

    private int stableCount;

    private volatile boolean shutdownTriggered;

    public SmartAdaptationTracker ( final JokerConfig config )
    {
        super( config );
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

        super.init( hook, execPlan );

        this.shutdownHook = hook;
        this.initialExecPlan = execPlan;
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

        super.onPeriod( execPlan, metrics );

        if ( initialMetrics == null )
        {
            checkState( stableCount == 0 );
            initialMetrics = metrics;
        }

        if ( stableCount++ > adaptationConfig.getStablePeriodCountToStop() )
        {
            finalExecPlan = execPlan;
            finalMetrics = metrics;
            LOGGER.info( "Decided to shutdown with flow execution plan: " + finalExecPlan.toSummaryString() );
            shutdownHook.shutdown();
            shutdownTriggered = true;
        }
    }

    @Override
    public void onExecPlanChange ( final FlowExecPlan newExecPlan )
    {
        checkArgument( newExecPlan != null );

        if ( !isEnabled() )
        {
            return;
        }

        checkRunning();

        super.onExecPlanChange( newExecPlan );

        stableCount = 0;
        LOGGER.info( "Stable period count is reset with new flow execution plan: " + newExecPlan.toSummaryString() );
    }

    public boolean isShutdownTriggered ()
    {
        return shutdownTriggered;
    }

    public FlowExecPlan getInitialExecPlan ()
    {
        // happens-before
        checkState( shutdownTriggered );
        return initialExecPlan;
    }

    public FlowMetrics getInitialMetrics ()
    {
        // happens-before
        checkState( shutdownTriggered );
        return initialMetrics;
    }

    public FlowExecPlan getFinalExecPlan ()
    {
        // happens-before
        checkState( shutdownTriggered );
        return finalExecPlan;
    }

    public FlowMetrics getFinalMetrics ()
    {
        // happens-before
        checkState( shutdownTriggered );
        return finalMetrics;
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
