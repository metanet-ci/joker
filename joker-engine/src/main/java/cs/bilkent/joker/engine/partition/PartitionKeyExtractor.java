package cs.bilkent.joker.engine.partition;

import cs.bilkent.joker.operator.Tuple;

public interface PartitionKeyExtractor
{

    PartitionKey getPartitionKey ( Tuple tuple );

    int getPartitionHash ( Tuple tuple );

}
