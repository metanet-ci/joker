package cs.bilkent.joker.engine.partition;

import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.partition.impl.PartitionKey;

public interface PartitionKeyExtractor
{

    PartitionKey getKey ( Tuple tuple );

    int getHash ( Tuple tuple );

}
