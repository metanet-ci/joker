package cs.bilkent.zanza.engine.tuplequeue;

import java.util.function.Consumer;

import cs.bilkent.zanza.operator.PortsToTuples;

public interface TupleQueuesConsumer extends Consumer<TupleQueue[]>
{

    PortsToTuples getPortsToTuples ();

}
