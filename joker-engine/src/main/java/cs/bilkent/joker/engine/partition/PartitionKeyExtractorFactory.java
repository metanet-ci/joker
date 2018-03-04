package cs.bilkent.joker.engine.partition;

import java.util.List;

public interface PartitionKeyExtractorFactory
{

    default PartitionKeyExtractor createPartitionKeyExtractor ( List<String> partitionFieldNames )
    {
        return createPartitionKeyExtractor( partitionFieldNames, partitionFieldNames.size() );
    }

    PartitionKeyExtractor createPartitionKeyExtractor ( List<String> partitionFieldNames, int forwardedKeySize );

}
