package cs.bilkent.zanza.engine.util;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.zanza.operator.spec.OperatorType;
import static cs.bilkent.zanza.operator.spec.OperatorType.PARTITIONED_STATEFUL;

public final class Preconditions
{
    private Preconditions ()
    {
    }

    public static void checkOperatorTypeAndPartitionKeyFieldNames ( final OperatorType operatorType,
                                                                    final List<String> partitionFieldNames )
    {
        final boolean nonPartitionedOperatorHasNoPartitionFieldNames =
                operatorType != PARTITIONED_STATEFUL && ( partitionFieldNames == null || partitionFieldNames.isEmpty() );
        final boolean partitionedOperatorHasPartitionFieldNames =
                operatorType == PARTITIONED_STATEFUL && partitionFieldNames != null && !partitionFieldNames.isEmpty();
        checkArgument( nonPartitionedOperatorHasNoPartitionFieldNames || partitionedOperatorHasPartitionFieldNames );
    }

}
