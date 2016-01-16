package cs.bilkent.zanza.engine.tuplequeue;

import cs.bilkent.zanza.operator.PortsToTuples;


public interface TupleQueueContext
{

    String getOperatorId ();

    void add ( PortsToTuples portsToTuples );

    void drain ( TupleQueuesConsumer tupleQueuesConsumer );

    void clear ();

}
