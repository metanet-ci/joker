package cs.bilkent.joker.engine.partition.impl;

import java.util.List;

import cs.bilkent.joker.engine.partition.PartitionKeyFunction;
import cs.bilkent.joker.operator.Tuple;

public class PartitionKeyFunction9 implements PartitionKeyFunction
{

    private final String fieldName1;

    private final String fieldName2;

    private final String fieldName3;

    private final String fieldName4;

    private final String fieldName5;

    private final String fieldName6;

    private final String fieldName7;

    private final String fieldName8;

    private final String fieldName9;

    public PartitionKeyFunction9 ( final List<String> partitionFieldNames )
    {
        this.fieldName1 = partitionFieldNames.get( 0 );
        this.fieldName2 = partitionFieldNames.get( 1 );
        this.fieldName3 = partitionFieldNames.get( 2 );
        this.fieldName4 = partitionFieldNames.get( 3 );
        this.fieldName5 = partitionFieldNames.get( 4 );
        this.fieldName6 = partitionFieldNames.get( 5 );
        this.fieldName7 = partitionFieldNames.get( 6 );
        this.fieldName8 = partitionFieldNames.get( 7 );
        this.fieldName9 = partitionFieldNames.get( 8 );
    }

    @Override
    public Object getPartitionKey ( final Tuple tuple )
    {
        return new PartitionKey9( tuple.getObject( fieldName1 ),
                                  tuple.getObject( fieldName2 ),
                                  tuple.getObject( fieldName3 ),
                                  tuple.getObject( fieldName4 ),
                                  tuple.getObject( fieldName5 ),
                                  tuple.getObject( fieldName6 ),
                                  tuple.getObject( fieldName7 ),
                                  tuple.getObject( fieldName8 ),
                                  tuple.getObject( fieldName9 ) );
    }

    @Override
    public int getPartitionHash ( final Tuple tuple )
    {
        return PartitionKey9.computeHashCode( tuple.getObject( fieldName1 ),
                                              tuple.getObject( fieldName2 ),
                                              tuple.getObject( fieldName3 ),
                                              tuple.getObject( fieldName4 ),
                                              tuple.getObject( fieldName5 ),
                                              tuple.getObject( fieldName6 ),
                                              tuple.getObject( fieldName7 ),
                                              tuple.getObject( fieldName8 ),
                                              tuple.getObject( fieldName9 ) );
    }

}
