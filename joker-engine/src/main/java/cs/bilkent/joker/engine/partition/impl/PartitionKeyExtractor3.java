package cs.bilkent.joker.engine.partition.impl;

import java.util.List;

import cs.bilkent.joker.engine.partition.PartitionKeyExtractor;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.partition.impl.PartitionKey;
import cs.bilkent.joker.partition.impl.PartitionKey3;
import static cs.bilkent.joker.partition.impl.PartitionKey3.computeHashCode;

public class PartitionKeyExtractor3 implements PartitionKeyExtractor
{

    private final String fieldName0;

    private final String fieldName1;

    private final String fieldName2;

    PartitionKeyExtractor3 ( final List<String> partitionFieldNames )
    {
        this.fieldName0 = partitionFieldNames.get( 0 );
        this.fieldName1 = partitionFieldNames.get( 1 );
        this.fieldName2 = partitionFieldNames.get( 2 );
    }

    @Override
    public PartitionKey getKey ( final Tuple tuple )
    {
        return new PartitionKey3( tuple.get( fieldName0 ), tuple.get( fieldName1 ), tuple.get( fieldName2 ) );
    }

    @Override
    public int getHash ( final Tuple tuple )
    {
        return computeHashCode( tuple.get( fieldName0 ), tuple.get( fieldName1 ), tuple.get( fieldName2 ) );
    }

}
