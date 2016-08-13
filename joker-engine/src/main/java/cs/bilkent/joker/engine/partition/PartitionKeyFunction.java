package cs.bilkent.joker.engine.partition;

import cs.bilkent.joker.operator.Tuple;

public interface PartitionKeyFunction
{

    Object getPartitionKey ( final Tuple tuple );

    int getPartitionHash ( final Tuple tuple );

}
