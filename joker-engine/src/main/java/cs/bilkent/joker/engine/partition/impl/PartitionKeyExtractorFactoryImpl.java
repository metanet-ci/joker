package cs.bilkent.joker.engine.partition.impl;

import java.util.List;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.joker.engine.partition.PartitionKeyExtractor;
import cs.bilkent.joker.engine.partition.PartitionKeyExtractorFactory;
import static java.lang.Math.min;

public class PartitionKeyExtractorFactoryImpl implements PartitionKeyExtractorFactory
{

    private static final int PARTITION_KEY_FUNCTION_COUNT = 4;

    private final Function<List<String>, PartitionKeyExtractor>[] defaultConstructors;

    public PartitionKeyExtractorFactoryImpl ()
    {
        this.defaultConstructors = new Function[ PARTITION_KEY_FUNCTION_COUNT + 1 ];
        this.defaultConstructors[ 1 ] = PartitionKeyExtractor1::new;
        this.defaultConstructors[ 2 ] = PartitionKeyExtractor2::new;
        this.defaultConstructors[ 3 ] = PartitionKeyExtractor3::new;
        this.defaultConstructors[ 4 ] = PartitionKeyExtractorN::new;
    }

    @Override
    public PartitionKeyExtractor createPartitionKeyExtractor ( final List<String> partitionFieldNames, final int forwardedKeySize )
    {
        checkArgument( partitionFieldNames.size() > 0, "no partition field names provided" );
        checkArgument( forwardedKeySize > 0 && forwardedKeySize <= partitionFieldNames.size(),
                       "invalid forwarded key size: %s",
                       forwardedKeySize );
        if ( partitionFieldNames.size() == forwardedKeySize )
        {
            final int i = min( partitionFieldNames.size(), PARTITION_KEY_FUNCTION_COUNT );
            return defaultConstructors[ i ].apply( partitionFieldNames );
        }

        if ( partitionFieldNames.size() == 2 )
        {
            return new PartitionKeyExtractor2Fwd1( partitionFieldNames );
        }
        else if ( partitionFieldNames.size() == 3 )
        {
            return forwardedKeySize == 1
                   ? new PartitionKeyExtractor3Fwd1( partitionFieldNames )
                   : new PartitionKeyExtractor3Fwd2( partitionFieldNames );
        }

        switch ( forwardedKeySize )
        {
            case 1:
                return new PartitionKeyExtractorNFwd1( partitionFieldNames );
            case 2:
                return new PartitionKeyExtractorNFwd2( partitionFieldNames );
            case 3:
                return new PartitionKeyExtractorNFwd3( partitionFieldNames );
            default:
                return new PartitionKeyExtractorNFwdM( partitionFieldNames, forwardedKeySize );
        }
    }

}
