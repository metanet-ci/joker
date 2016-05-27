package cs.bilkent.zanza.engine.tuplequeue;

import cs.bilkent.zanza.engine.config.ZanzaConfig;
import cs.bilkent.zanza.flow.OperatorDefinition;
import cs.bilkent.zanza.operator.scheduling.SchedulingStrategy;

public interface TupleQueueDrainerPool
{

    void init ( ZanzaConfig config, OperatorDefinition operatorDefinition );

    TupleQueueDrainer acquire ( SchedulingStrategy strategy );

    void release ( TupleQueueDrainer drainer );

}
