package cs.bilkent.joker.engine.partition.impl;

import java.util.List;

import cs.bilkent.joker.engine.partition.PartitionKey;
import cs.bilkent.joker.engine.partition.PartitionKeyExtractor;
import static cs.bilkent.joker.engine.partition.impl.PartitionKeyNFwd2.computePartitionHash;
import cs.bilkent.joker.operator.Tuple;

public class PartitionKeyExtractorNFwd2 implements PartitionKeyExtractor
{

    private final List<String> partitionFieldNames;

    private final String field0;

    private final String field1;

    public PartitionKeyExtractorNFwd2 ( final List<String> partitionFieldNames )
    {
        this.partitionFieldNames = partitionFieldNames;
        this.field0 = partitionFieldNames.get( 0 );
        this.field1 = partitionFieldNames.get( 1 );
    }

    @Override
    public PartitionKey getPartitionKey ( final Tuple tuple )
    {
        return new PartitionKeyNFwd2( tuple, partitionFieldNames );
    }

    @Override
    public int getPartitionHash ( final Tuple tuple )
    {
        return computePartitionHash( tuple.get( field0 ), tuple.get( field1 ) );
    }

}
