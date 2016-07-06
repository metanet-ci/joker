package cs.bilkent.zanza.engine.partition.impl;

import java.util.List;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.zanza.engine.partition.PartitionKeyFunction;
import cs.bilkent.zanza.engine.partition.PartitionKeyFunctionFactory;
import static java.lang.Math.min;

public class PartitionKeyFunctionFactoryImpl implements PartitionKeyFunctionFactory
{

    private static final int PARTITION_KEY_FUNCTION_COUNT = 11;

    private final Function<List<String>, PartitionKeyFunction>[] partitionKeyFunctionConstructors;

    public PartitionKeyFunctionFactoryImpl ()
    {

        this.partitionKeyFunctionConstructors = new Function[ PARTITION_KEY_FUNCTION_COUNT + 1 ];
        this.partitionKeyFunctionConstructors[ 1 ] = PartitionKeyFunction1::new;
        this.partitionKeyFunctionConstructors[ 2 ] = PartitionKeyFunction2::new;
        this.partitionKeyFunctionConstructors[ 3 ] = PartitionKeyFunction3::new;
        this.partitionKeyFunctionConstructors[ 4 ] = PartitionKeyFunction4::new;
        this.partitionKeyFunctionConstructors[ 5 ] = PartitionKeyFunction5::new;
        this.partitionKeyFunctionConstructors[ 6 ] = PartitionKeyFunction6::new;
        this.partitionKeyFunctionConstructors[ 7 ] = PartitionKeyFunction7::new;
        this.partitionKeyFunctionConstructors[ 8 ] = PartitionKeyFunction8::new;
        this.partitionKeyFunctionConstructors[ 9 ] = PartitionKeyFunction9::new;
        this.partitionKeyFunctionConstructors[ 10 ] = PartitionKeyFunction10::new;
        this.partitionKeyFunctionConstructors[ 11 ] = PartitionKeyFunctionN::new;
    }

    @Override
    public PartitionKeyFunction createPartitionKeyFunction ( final List<String> partitionFieldNames )
    {
        checkArgument( partitionFieldNames.size() > 0 );
        final int i = min( partitionFieldNames.size(), PARTITION_KEY_FUNCTION_COUNT );
        return partitionKeyFunctionConstructors[ i ].apply( partitionFieldNames );
    }

}
