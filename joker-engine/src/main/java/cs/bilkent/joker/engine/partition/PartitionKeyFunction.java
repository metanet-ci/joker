package cs.bilkent.joker.engine.partition;

import cs.bilkent.joker.operator.Tuple;

public interface PartitionKeyFunction
{

    Object getPartitionKey ( Tuple tuple );

    int getPartitionHash ( Tuple tuple );

}
