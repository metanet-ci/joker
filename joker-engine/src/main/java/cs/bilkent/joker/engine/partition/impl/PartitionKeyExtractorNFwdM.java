package cs.bilkent.joker.engine.partition.impl;

import java.util.List;

import cs.bilkent.joker.engine.partition.PartitionKey;
import cs.bilkent.joker.engine.partition.PartitionKeyExtractor;
import static cs.bilkent.joker.engine.partition.impl.PartitionKeyNFwdM.computePartitionHash;
import cs.bilkent.joker.operator.Tuple;

public class PartitionKeyExtractorNFwdM implements PartitionKeyExtractor
{

    private final List<String> partitionFieldNames;

    private final int forwardKeyLimit;

    PartitionKeyExtractorNFwdM ( final List<String> partitionFieldNames, final int forwardKeyLimit )
    {
        this.partitionFieldNames = partitionFieldNames;
        this.forwardKeyLimit = forwardKeyLimit;
    }

    @Override
    public PartitionKey getPartitionKey ( final Tuple tuple )
    {
        return new PartitionKeyNFwdM( tuple, partitionFieldNames, forwardKeyLimit );
    }

    @Override
    public int getPartitionHash ( final Tuple tuple )
    {
        return computePartitionHash( tuple, partitionFieldNames, forwardKeyLimit );
    }

}
