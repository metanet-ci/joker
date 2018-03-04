package cs.bilkent.joker.engine.adaptation;

import java.util.concurrent.Future;

import cs.bilkent.joker.engine.flow.FlowExecPlan;
import cs.bilkent.joker.engine.metric.FlowMetrics;

public interface AdaptationTracker
{

    void init ( ShutdownHook hook, FlowExecPlan execPlan );

    void onPeriod ( FlowExecPlan execPlan, FlowMetrics metrics );

    void onExecPlanChange ( FlowExecPlan newExecPlan );

    @FunctionalInterface
    interface ShutdownHook
    {

        Future<Void> shutdown ();

    }

}
