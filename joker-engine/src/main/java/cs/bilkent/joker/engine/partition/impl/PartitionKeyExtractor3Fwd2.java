package cs.bilkent.joker.engine.partition.impl;

import java.util.List;

import cs.bilkent.joker.engine.partition.PartitionKey;
import cs.bilkent.joker.engine.partition.PartitionKeyExtractor;
import static cs.bilkent.joker.engine.partition.impl.PartitionKey3Fwd2.computePartitionHashCode;
import cs.bilkent.joker.operator.Tuple;

public class PartitionKeyExtractor3Fwd2 implements PartitionKeyExtractor
{

    private final String fieldName0;

    private final String fieldName1;

    private final String fieldName2;

    public PartitionKeyExtractor3Fwd2 ( final List<String> partitionFieldNames )
    {
        this.fieldName0 = partitionFieldNames.get( 0 );
        this.fieldName1 = partitionFieldNames.get( 1 );
        this.fieldName2 = partitionFieldNames.get( 2 );
    }

    @Override
    public PartitionKey getPartitionKey ( final Tuple tuple )
    {
        return new PartitionKey3Fwd2( tuple.getObject( fieldName0 ), tuple.getObject( fieldName1 ), tuple.getObject( fieldName2 ) );
    }

    @Override
    public int getPartitionHash ( final Tuple tuple )
    {
        return computePartitionHashCode( tuple.getObject( fieldName0 ), tuple.getObject( fieldName1 ) );
    }

}
