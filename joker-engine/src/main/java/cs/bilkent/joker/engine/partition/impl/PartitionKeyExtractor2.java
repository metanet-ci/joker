package cs.bilkent.joker.engine.partition.impl;

import java.util.List;

import cs.bilkent.joker.engine.partition.PartitionKeyExtractor;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.partition.impl.PartitionKey;
import cs.bilkent.joker.partition.impl.PartitionKey2;
import static cs.bilkent.joker.partition.impl.PartitionKey2.computeHashCode;

public class PartitionKeyExtractor2 implements PartitionKeyExtractor
{

    private final String fieldName0;

    private final String fieldName1;

    PartitionKeyExtractor2 ( final List<String> partitionFieldNames )
    {
        this.fieldName0 = partitionFieldNames.get( 0 );
        this.fieldName1 = partitionFieldNames.get( 1 );
    }

    @Override
    public PartitionKey getKey ( final Tuple tuple )
    {
        return new PartitionKey2( tuple.getObject( fieldName0 ), tuple.get( fieldName1 ) );
    }

    @Override
    public int getHash ( final Tuple tuple )
    {
        return computeHashCode( tuple.get( fieldName0 ), tuple.getObject( fieldName1 ) );
    }

}
