package cs.bilkent.joker.engine.partition.impl;

import java.util.List;

import cs.bilkent.joker.engine.partition.PartitionKeyFunction;
import cs.bilkent.joker.operator.Tuple;

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
