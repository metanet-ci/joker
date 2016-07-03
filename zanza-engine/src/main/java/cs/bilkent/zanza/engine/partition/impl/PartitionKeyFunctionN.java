package cs.bilkent.zanza.engine.partition.impl;

import java.util.List;

import cs.bilkent.zanza.engine.partition.PartitionKeyFunction;
import cs.bilkent.zanza.operator.Tuple;

public class PartitionKeyFunctionN implements PartitionKeyFunction
{

    private final List<String> partitionFieldNames;

    private final int size;

    public PartitionKeyFunctionN ( final List<String> partitionFieldNames )
    {
        this.partitionFieldNames = partitionFieldNames;
        this.size = partitionFieldNames.size();
    }

    @Override
    public Object getPartitionKey ( final Tuple tuple )
    {
        final Object[] vals = new Object[ size ];
        for ( int i = 0; i < size; i++ )
        {
            vals[ i ] = tuple.getObject( partitionFieldNames.get( i ) );
        }

        return vals;
    }

    @Override
    public int getPartitionHash ( final Tuple tuple )
    {
        return getPartitionKey( tuple ).hashCode();
    }

}
