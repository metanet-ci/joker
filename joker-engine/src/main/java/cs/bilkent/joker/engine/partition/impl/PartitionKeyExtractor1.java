package cs.bilkent.joker.engine.partition.impl;

import java.util.List;

import cs.bilkent.joker.engine.partition.PartitionKey;
import cs.bilkent.joker.engine.partition.PartitionKeyExtractor;
import static cs.bilkent.joker.engine.partition.impl.PartitionKey1.computeHashCode;
import cs.bilkent.joker.operator.Tuple;

public class PartitionKeyExtractor1 implements PartitionKeyExtractor
{

    private final String fieldName;

    public PartitionKeyExtractor1 ( final List<String> partitionFieldNames )
    {
        this.fieldName = partitionFieldNames.get( 0 );
    }

    @Override
    public PartitionKey getPartitionKey ( final Tuple tuple )
    {
        return new PartitionKey1( tuple.getObject( fieldName ) );
    }

    @Override
    public int getPartitionHash ( final Tuple tuple )
    {
        return computeHashCode( tuple.getObject( fieldName ) );
    }

}
