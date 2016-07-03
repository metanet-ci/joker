package cs.bilkent.zanza.engine.partition.impl;

import java.util.List;

import cs.bilkent.zanza.engine.partition.PartitionKeyFunction;
import cs.bilkent.zanza.operator.Tuple;

public class PartitionKeyFunction1 implements PartitionKeyFunction
{

    private final String fieldName;

    public PartitionKeyFunction1 ( final List<String> partitionFieldNames )
    {
        this.fieldName = partitionFieldNames.get( 0 );
    }

    @Override
    public Object getPartitionKey ( final Tuple tuple )
    {
        return tuple.getObject( fieldName );
    }

    @Override
    public int getPartitionHash ( final Tuple tuple )
    {
        return getPartitionKey( tuple ).hashCode();
    }

}
