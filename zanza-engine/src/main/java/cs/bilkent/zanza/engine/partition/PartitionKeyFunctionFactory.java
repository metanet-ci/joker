package cs.bilkent.zanza.engine.partition;

import java.util.List;

public interface PartitionKeyFunctionFactory
{

    PartitionKeyFunction createPartitionKeyFunction ( List<String> partitionFieldNames );

}
