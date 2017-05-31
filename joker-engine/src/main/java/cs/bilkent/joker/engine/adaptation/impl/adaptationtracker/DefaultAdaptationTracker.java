package cs.bilkent.joker.engine.adaptation.impl.adaptationtracker;

import javax.inject.Inject;
import javax.inject.Singleton;

import cs.bilkent.joker.engine.adaptation.AdaptationTracker;
import static cs.bilkent.joker.engine.adaptation.impl.adaptationtracker.Visualizer.visualize;
import cs.bilkent.joker.engine.config.AdaptationConfig;
import cs.bilkent.joker.engine.config.JokerConfig;
import cs.bilkent.joker.engine.flow.FlowExecutionPlan;
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
    public void init ( final ShutdownHook hook, final FlowExecutionPlan flowExecutionPlan )
    {
        checkArgument( hook != null );
        checkArgument( flowExecutionPlan != null );

        if ( !isEnabled() )
        {
            return;
        }

        visualize( flowExecutionPlan, currentDir );
    }

    @Override
    public void onPeriod ( final FlowExecutionPlan flowExecutionPlan, final FlowMetrics flowMetrics )
    {
        checkArgument( flowExecutionPlan != null );
        checkArgument( flowMetrics != null );
    }

    @Override
    public void onFlowExecutionPlanChange ( final FlowExecutionPlan newFlowExecutionPlan )
    {
        checkArgument( newFlowExecutionPlan != null );

        if ( !isEnabled() )
        {
            return;
        }

        visualize( newFlowExecutionPlan, currentDir );
    }

    private boolean isEnabled ()
    {
        return adaptationConfig.isAdaptationEnabled() && adaptationConfig.isVisualizationEnabled();
    }

}
