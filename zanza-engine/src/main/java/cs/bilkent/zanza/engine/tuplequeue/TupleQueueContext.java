package cs.bilkent.zanza.engine.tuplequeue;

import java.util.List;

import cs.bilkent.zanza.operator.PortsToTuples;
import cs.bilkent.zanza.scheduling.ScheduleWhenTuplesAvailable.PortToTupleCount;


public interface TupleQueueContext
{

    String getOperatorId ();

    void add ( PortsToTuples portsToTuples );

    List<PortToTupleCount> tryAdd ( PortsToTuples portsToTuples, long timeoutInMillis );

    void drain ( TupleQueuesConsumer tupleQueuesConsumer );

    void clear ();

}
