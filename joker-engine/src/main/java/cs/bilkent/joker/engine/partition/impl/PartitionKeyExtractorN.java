package cs.bilkent.joker.engine.partition.impl;

import java.util.List;

import cs.bilkent.joker.engine.partition.PartitionKeyExtractor;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.partition.impl.PartitionKey;
import cs.bilkent.joker.partition.impl.PartitionKeyN;
import static cs.bilkent.joker.partition.impl.PartitionKeyN.computeHashCode;

public class PartitionKeyExtractorN implements PartitionKeyExtractor
{

    private final List<String> partitionFieldNames;

    PartitionKeyExtractorN ( final List<String> partitionFieldNames )
    {
        this.partitionFieldNames = partitionFieldNames;
    }

    @Override
    public PartitionKey getPartitionKey ( final Tuple tuple )
    {
        return new PartitionKeyN( tuple, partitionFieldNames );
    }

    @Override
    public int getPartitionHash ( final Tuple tuple )
    {
        return computeHashCode( tuple, partitionFieldNames );
    }

}
