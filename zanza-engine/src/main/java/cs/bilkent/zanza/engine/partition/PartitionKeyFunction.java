package cs.bilkent.zanza.engine.partition;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

import cs.bilkent.zanza.operator.Tuple;

public class PartitionKeyFunction implements Function<Tuple, Object>
{

    private final BiFunction<Tuple, List<String>, Object>[] partitionKeyFunctions = new BiFunction[ 11 ];

    private final List<String> partitionFieldNames;

    public PartitionKeyFunction ( final List<String> partitionFieldNames )
    {
        this.partitionFieldNames = partitionFieldNames;
        partitionKeyFunctions[ 0 ] = ( tuple, f ) -> null;
        partitionKeyFunctions[ 1 ] = ( tuple, f ) -> tuple.getObject( f.get( 0 ) );
        partitionKeyFunctions[ 3 ] = PartitionKey3::new;
        partitionKeyFunctions[ 4 ] = PartitionKey4::new;
        partitionKeyFunctions[ 5 ] = PartitionKey5::new;
        partitionKeyFunctions[ 6 ] = PartitionKey6::new;
        partitionKeyFunctions[ 7 ] = PartitionKey7::new;
        partitionKeyFunctions[ 8 ] = PartitionKey8::new;
        partitionKeyFunctions[ 9 ] = PartitionKey9::new;
        partitionKeyFunctions[ 10 ] = PartitionKey10::new;
    }

    @Override
    public Object apply ( final Tuple tuple )
    {
        final int s = partitionFieldNames.size();
        if ( s <= 10 )
        {
            return partitionKeyFunctions[ s ].apply( tuple, partitionFieldNames );
        }

        final List<Object> vals = new ArrayList<>( s );
        for ( String partitionFieldName : partitionFieldNames )
        {
            vals.add( tuple.get( partitionFieldName ) );
        }

        return vals;
    }

}
