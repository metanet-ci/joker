package cs.bilkent.joker.engine.partition.impl;

import java.util.List;

import cs.bilkent.joker.engine.partition.PartitionKey;
import cs.bilkent.joker.engine.partition.PartitionKeyExtractor;
import static cs.bilkent.joker.engine.partition.impl.PartitionKeyN.computeHashCode;
import cs.bilkent.joker.operator.Tuple;

public class PartitionKeyExtractorN implements PartitionKeyExtractor
{

    private final List<String> partitionFieldNames;

    public PartitionKeyExtractorN ( final List<String> partitionFieldNames )
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
