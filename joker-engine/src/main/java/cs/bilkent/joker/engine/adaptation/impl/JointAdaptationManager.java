package cs.bilkent.joker.engine.adaptation.impl;

import java.util.List;
import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import static cs.bilkent.joker.JokerModule.LATENCY_OPTIMIZING_ADAPTATION_MANAGER_NAME;
import static cs.bilkent.joker.JokerModule.THROUGHPUT_OPTIMIZING_ADAPTATION_MANAGER_NAME;
import cs.bilkent.joker.engine.adaptation.AdaptationAction;
import cs.bilkent.joker.engine.adaptation.AdaptationManager;
import cs.bilkent.joker.engine.flow.RegionExecPlan;
import cs.bilkent.joker.engine.metric.FlowMetrics;
import cs.bilkent.joker.flow.FlowDef;

@NotThreadSafe
@Singleton
public class JointAdaptationManager implements AdaptationManager
{

    private final AdaptationManager throughputOptimizingAdaptationManager;

    private final AdaptationManager latencyOptimizingAdaptationManager;

    private boolean optimizingThroughput = true;

    private boolean skipLatencyOptimization;

    @Inject
    public JointAdaptationManager ( @Named( THROUGHPUT_OPTIMIZING_ADAPTATION_MANAGER_NAME ) final AdaptationManager
                                                throughputOptimizingAdaptationManager,
                                    @Named( LATENCY_OPTIMIZING_ADAPTATION_MANAGER_NAME ) final AdaptationManager
                                            latencyOptimizingAdaptationManager )
    {
        this.throughputOptimizingAdaptationManager = throughputOptimizingAdaptationManager;
        this.latencyOptimizingAdaptationManager = latencyOptimizingAdaptationManager;
    }

    @Override
    public void initialize ( final FlowDef flowDef, final List<RegionExecPlan> execPlans )
    {
        throughputOptimizingAdaptationManager.initialize( flowDef, execPlans );
        latencyOptimizingAdaptationManager.initialize( flowDef, execPlans );
    }

    @Override
    public void disableAdaptation ()
    {
        throughputOptimizingAdaptationManager.disableAdaptation();
        latencyOptimizingAdaptationManager.disableAdaptation();
    }

    @Override
    public List<AdaptationAction> adapt ( final List<RegionExecPlan> execPlans, final FlowMetrics metrics )
    {
        List<AdaptationAction> actions;
        if ( optimizingThroughput )
        {
            actions = throughputOptimizingAdaptationManager.adapt( execPlans, metrics );
            if ( actions.size() > 0 )
            {
                skipLatencyOptimization = true;
                return actions;
            }
            else if ( skipLatencyOptimization )
            {
                skipLatencyOptimization = false;
                return actions;
            }
        }

        actions = latencyOptimizingAdaptationManager.adapt( execPlans, metrics );
        optimizingThroughput = actions.isEmpty();

        return actions;
    }

    boolean isOptimizingThroughput ()
    {
        return optimizingThroughput;
    }

}
