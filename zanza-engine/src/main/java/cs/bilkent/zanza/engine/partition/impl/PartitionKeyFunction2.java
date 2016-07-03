package cs.bilkent.zanza.engine.partition.impl;

import java.util.List;

import cs.bilkent.zanza.engine.partition.PartitionKeyFunction;
import cs.bilkent.zanza.operator.Tuple;

public class PartitionKeyFunction2 implements PartitionKeyFunction
{

    private final String fieldName1;

    private final String fieldName2;

    public PartitionKeyFunction2 ( final List<String> partitionFieldNames )
    {
        this.fieldName1 = partitionFieldNames.get( 0 );
        this.fieldName2 = partitionFieldNames.get( 1 );
    }

    @Override
    public Object getPartitionKey ( final Tuple tuple )
    {
        return new PartitionKey2( tuple.getObject( fieldName1 ), tuple.getObject( fieldName2 ) );
    }

    @Override
    public int getPartitionHash ( final Tuple tuple )
    {
        return PartitionKey2.computeHashCode( tuple.getObject( fieldName1 ), tuple.getObject( fieldName2 ) );
    }

}
