package cs.bilkent.zanza.engine.pipeline;

import cs.bilkent.zanza.operator.scheduling.SchedulingStrategy;

public interface OperatorInstanceListener
{

    void onStatusChange ( String operatorId, OperatorInstanceStatus status );

    void onSchedulingStrategyChange ( String operatorId, SchedulingStrategy newSchedulingStrategy );

}
