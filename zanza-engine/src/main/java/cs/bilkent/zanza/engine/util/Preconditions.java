package cs.bilkent.zanza.engine.util;

import static com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.zanza.operator.PartitionKeyExtractor;
import cs.bilkent.zanza.operator.spec.OperatorType;
import static cs.bilkent.zanza.operator.spec.OperatorType.PARTITIONED_STATEFUL;

public final class Preconditions
{
    private Preconditions ()
    {
    }

    public static void checkOperatorTypeAndPartitionKeyExtractor ( final OperatorType operatorType,
                                                                   final PartitionKeyExtractor partitionKeyExtractor )
    {
        final boolean nonPartitionedOperatorHasNoPartitionKeyExtractor =
                operatorType != PARTITIONED_STATEFUL && partitionKeyExtractor == null;
        final boolean partitionedOperatorHasPartitionKeyExtractor = operatorType == PARTITIONED_STATEFUL && partitionKeyExtractor != null;
        checkArgument( nonPartitionedOperatorHasNoPartitionKeyExtractor || partitionedOperatorHasPartitionKeyExtractor );
    }

}
