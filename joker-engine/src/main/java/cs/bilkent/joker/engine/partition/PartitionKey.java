package cs.bilkent.joker.engine.partition;

import java.util.List;

public interface PartitionKey extends List<Object>
{

    int partitionHashCode ();

}
