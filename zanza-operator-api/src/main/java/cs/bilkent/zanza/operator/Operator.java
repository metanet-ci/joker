package cs.bilkent.zanza.operator;

import cs.bilkent.zanza.operator.scheduling.SchedulingStrategy;

public interface Operator
{
    SchedulingStrategy init ( final OperatorContext context );

    ProcessingResult process ( PortsToTuples portsToTuples, InvocationReason reason );

    default void destroy ()
    {

    }

}
