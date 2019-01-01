package cs.bilkent.joker.engine.partition.impl;

import java.util.List;

import cs.bilkent.joker.engine.partition.PartitionKeyExtractor;
import static cs.bilkent.joker.engine.partition.impl.PartitionKeyNFwd1.computePartitionHash;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.partition.impl.PartitionKey;

public class PartitionKeyExtractorNFwd1 implements PartitionKeyExtractor
{

    private final List<String> partitionFieldNames;

    private final String field0;

    PartitionKeyExtractorNFwd1 ( final List<String> partitionFieldNames )
    {
        this.partitionFieldNames = partitionFieldNames;
        this.field0 = partitionFieldNames.get( 0 );
    }

    @Override
    public PartitionKey getKey ( final Tuple tuple )
    {
        return new PartitionKeyNFwd1( tuple, partitionFieldNames );
    }

    @Override
    public int getHash ( final Tuple tuple )
    {
        return computePartitionHash( tuple.get( field0 ) );
    }

}
