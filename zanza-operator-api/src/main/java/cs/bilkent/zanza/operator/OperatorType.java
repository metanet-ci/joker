package cs.bilkent.zanza.operator;

import cs.bilkent.zanza.operator.kvstore.KVStore;

/**
 * Defines type of an {@link Operator} within {@link OperatorSpec}
 *
 * @see Operator
 * @see OperatorSpec
 * @see KVStore
 */
public enum OperatorType
{
    /**
     * The engine may create as many instances as it decides of an operator defined as {@code STATELESS}
     * <p>
     * The engine does not provide any state manipulation capabilities to an operator defined as {@code STATELESS}.
     */
    STATELESS,

    /**
     * The engine creates multiple instances of the operator and divides the key space into partitions
     * to process each partition using a single instance of the operator.
     *
     * A {@link KVStore} implementation is provided to an operator defined as {@code STATEFUL}.
     * Operator must manage its local state using the provided {@link KVStore} implementation.
     * Then the engine handles the safety and migrations of the local state.
     *
     * A {@link PartitionKeyExtractor} implementation must be provided during the operator composition.
     *
     * @see PartitionKeyExtractor
     * @see KVStore
     */
    PARTITIONED_STATEFUL,

    /**
     * The engine processes all the keys using a single instance of the operator.
     *
     * A {@link KVStore} implementation is provided to an operator defined as {@code STATEFUL}.
     * Operator must manage its local state using the provided {@link KVStore} implementation.
     * Then the engine handles the safety and migrations of the local state.
     * @see KVStore
     */
    STATEFUL
}
