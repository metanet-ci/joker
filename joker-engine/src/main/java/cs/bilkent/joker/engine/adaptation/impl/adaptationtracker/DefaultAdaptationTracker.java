package cs.bilkent.joker.engine.adaptation.impl.adaptationtracker;

import javax.inject.Inject;
import javax.inject.Singleton;

import cs.bilkent.joker.engine.adaptation.AdaptationTracker;
import static cs.bilkent.joker.engine.adaptation.impl.adaptationtracker.Visualizer.visualize;
import cs.bilkent.joker.engine.config.AdaptationConfig;
import cs.bilkent.joker.engine.config.JokerConfig;
import cs.bilkent.joker.engine.flow.FlowExecPlan;
import cs.bilkent.joker.engine.metric.FlowMetrics;
import static cs.bilkent.joker.impl.com.google.common.base.Preconditions.checkArgument;

@Singleton
public class DefaultAdaptationTracker implements AdaptationTracker
{

    private final String currentDir;

    final AdaptationConfig adaptationConfig;

    @Inject
    public DefaultAdaptationTracker ( final JokerConfig jokerConfig )
    {
        this.currentDir = System.getProperty( "user.dir" );
        this.adaptationConfig = jokerConfig.getAdaptationConfig();
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

        visualize( execPlan, currentDir );
    }

    @Override
    public void onPeriod ( final FlowExecPlan execPlan, final FlowMetrics metrics )
    {
        checkArgument( execPlan != null );
        checkArgument( metrics != null );
    }

    @Override
    public void onExecPlanChange ( final FlowExecPlan newExecPlan )
    {
        checkArgument( newExecPlan != null );

        if ( !isEnabled() )
        {
            return;
        }

        visualize( newExecPlan, currentDir );
    }

    private boolean isEnabled ()
    {
        return adaptationConfig.isAdaptationEnabled() && adaptationConfig.isVisualizationEnabled();
    }

}
