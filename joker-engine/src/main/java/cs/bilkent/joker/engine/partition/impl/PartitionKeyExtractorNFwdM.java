package cs.bilkent.joker.engine.partition.impl;

import java.util.List;

import cs.bilkent.joker.engine.partition.PartitionKeyExtractor;
import static cs.bilkent.joker.engine.partition.impl.PartitionKeyNFwdM.computePartitionHash;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.partition.impl.PartitionKey;

public class PartitionKeyExtractorNFwdM implements PartitionKeyExtractor
{

    private final List<String> partitionFieldNames;

    private final int forwardedKeySize;

    PartitionKeyExtractorNFwdM ( final List<String> partitionFieldNames, final int forwardedKeySize )
    {
        this.partitionFieldNames = partitionFieldNames;
        this.forwardedKeySize = forwardedKeySize;
    }

    @Override
    public PartitionKey getKey ( final Tuple tuple )
    {
        return new PartitionKeyNFwdM( tuple, partitionFieldNames, forwardedKeySize );
    }

    @Override
    public int getHash ( final Tuple tuple )
    {
        return computePartitionHash( tuple, partitionFieldNames, forwardedKeySize );
    }

}
