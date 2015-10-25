package cs.bilkent.zanza.operator;

import java.util.function.Function;

/**
 * Used for extracting partition keys from the {@link Tuple} instances.
 * Partition keys are used for forwarding tuples to instances of {@link OperatorType#PARTITIONED_STATEFUL} operators.
 *
 * @see OperatorType
 * @see PartitionKeyExtractors for built-in implementations
 */

public interface PartitionKeyExtractor extends Function<Tuple, Object>
{

}
