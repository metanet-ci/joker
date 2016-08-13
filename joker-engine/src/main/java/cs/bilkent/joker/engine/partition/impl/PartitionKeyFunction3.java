package cs.bilkent.joker.engine.partition.impl;

import java.util.List;

import cs.bilkent.joker.engine.partition.PartitionKeyFunction;
import cs.bilkent.joker.operator.Tuple;

public class PartitionKeyFunction3 implements PartitionKeyFunction
{

    private final String fieldName1;

    private final String fieldName2;

    private final String fieldName3;

    public PartitionKeyFunction3 ( final List<String> partitionFieldNames )
    {
        this.fieldName1 = partitionFieldNames.get( 0 );
        this.fieldName2 = partitionFieldNames.get( 1 );
        this.fieldName3 = partitionFieldNames.get( 2 );
    }

    @Override
    public Object getPartitionKey ( final Tuple tuple )
    {
        return new PartitionKey3( tuple.getObject( fieldName1 ), tuple.getObject( fieldName2 ), tuple.getObject( fieldName3 ) );
    }

    @Override
    public int getPartitionHash ( final Tuple tuple )
    {
        return PartitionKey3.computeHashCode( tuple.getObject( fieldName1 ), tuple.getObject( fieldName2 ), tuple.getObject( fieldName3 ) );
    }

}
