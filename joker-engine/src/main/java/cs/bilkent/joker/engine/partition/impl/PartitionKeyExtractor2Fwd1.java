package cs.bilkent.joker.engine.partition.impl;

import java.util.List;

import cs.bilkent.joker.engine.partition.PartitionKey;
import cs.bilkent.joker.engine.partition.PartitionKeyExtractor;
import static cs.bilkent.joker.engine.partition.impl.PartitionKey2Fwd1.computePartitionHashCode;
import cs.bilkent.joker.operator.Tuple;

public class PartitionKeyExtractor2Fwd1 implements PartitionKeyExtractor
{

    private final String fieldName0;

    private final String fieldName1;

    public PartitionKeyExtractor2Fwd1 ( final List<String> partitionFieldNames )
    {
        this.fieldName0 = partitionFieldNames.get( 0 );
        this.fieldName1 = partitionFieldNames.get( 1 );
    }

    @Override
    public PartitionKey getPartitionKey ( final Tuple tuple )
    {
        return new PartitionKey2Fwd1( tuple.getObject( fieldName0 ), tuple.getObject( fieldName1 ) );
    }

    @Override
    public int getPartitionHash ( final Tuple tuple )
    {
        return computePartitionHashCode( tuple.getObject( fieldName0 ) );
    }

}
