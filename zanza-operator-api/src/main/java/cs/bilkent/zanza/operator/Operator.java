package cs.bilkent.zanza.operator;

import cs.bilkent.zanza.operator.scheduling.SchedulingStrategy;

public interface Operator
{
    ProcessingResult process ( PortsToTuples portsToTuples, InvocationReason reason );

    SchedulingStrategy init ( final OperatorContext context );

    default void destroy ()
    {

    }

}
