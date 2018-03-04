package cs.bilkent.joker.partition.impl;

import java.util.List;

public interface PartitionKey extends List<Object>
{

    int partitionHashCode ();

}
