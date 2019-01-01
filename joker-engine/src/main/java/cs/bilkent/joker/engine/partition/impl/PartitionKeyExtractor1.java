package cs.bilkent.joker.engine.partition.impl;

import java.util.List;

import cs.bilkent.joker.engine.partition.PartitionKeyExtractor;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.partition.impl.PartitionKey;
import cs.bilkent.joker.partition.impl.PartitionKey1;
import static cs.bilkent.joker.partition.impl.PartitionKey1.computeHashCode;

public class PartitionKeyExtractor1 implements PartitionKeyExtractor
{

    private final String fieldName;

    public PartitionKeyExtractor1 ( final List<String> partitionFieldNames )
    {
        this.fieldName = partitionFieldNames.get( 0 );
    }

    @Override
    public PartitionKey getKey ( final Tuple tuple )
    {
        return new PartitionKey1( tuple.getObject( fieldName ) );
    }

    @Override
    public int getHash ( final Tuple tuple )
    {
        return computeHashCode( tuple.getObject( fieldName ) );
    }

}
