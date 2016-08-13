package cs.bilkent.joker.engine.partition;

import java.util.List;

public interface PartitionKeyFunctionFactory
{

    PartitionKeyFunction createPartitionKeyFunction ( List<String> partitionFieldNames );

}
