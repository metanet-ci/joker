package cs.bilkent.joker.engine.partition;

import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.partition.impl.PartitionKey;

public interface PartitionKeyExtractor
{

    PartitionKey getPartitionKey ( Tuple tuple );

    int getPartitionHash ( Tuple tuple );

}
