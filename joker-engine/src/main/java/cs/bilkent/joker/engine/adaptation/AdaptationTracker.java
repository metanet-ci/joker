package cs.bilkent.joker.engine.adaptation;

import java.util.concurrent.Future;

import cs.bilkent.joker.engine.flow.FlowExecutionPlan;
import cs.bilkent.joker.engine.metric.FlowMetrics;

public interface AdaptationTracker
{

    void init ( ShutdownHook hook, FlowExecutionPlan initialFlowExecutionPlan );

    void onPeriod ( FlowExecutionPlan flowExecutionPlan, FlowMetrics flowMetrics );

    void onFlowExecutionPlanChange ( FlowExecutionPlan newFlowExecutionPlan );

    @FunctionalInterface
    interface ShutdownHook
    {

        Future<Void> shutdown ();

    }

}
