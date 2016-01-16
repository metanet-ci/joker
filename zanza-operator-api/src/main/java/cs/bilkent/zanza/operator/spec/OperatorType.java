package cs.bilkent.zanza.operator.spec;


import cs.bilkent.zanza.kvstore.KVStore;
import cs.bilkent.zanza.operator.InvocationContext;
import cs.bilkent.zanza.operator.Operator;


/**
 * Defines type of an {@link Operator} within {@link OperatorSpec}
 *
 * @see OperatorSpec
 * @see Operator
 * @see KVStore
 */
public enum OperatorType
{
    /**
     * The engine may create as many instances as it decides of an operator defined as {@code STATELESS}
     * <p>
     * The engine does not provide any state manipulation capabilities to an operator defined as {@code STATELESS}.
     * No {@link KVStore} implementation is given to the {@link Operator#process(InvocationContext)} for an invocation.
     */
    STATELESS,

    /**
     * The engine creates multiple instances of the operator and divides the key space into partitions
     * to process each partition using a single instance of the operator.
     * <p>
     * Operator must manage its local state using the provided {@link KVStore} implementation provided within
     * the {@link InvocationContext}. Then the engine handles the safety and migrations of the local state.
     * <p>
     * The engine does not provide any ordering guarantees for the tuples with different partition keys. However,
     * ordering guarantees are given for the tuples of a particular partition key.
     * <p>
     * For an operator defined as {@code PARTITIONED_STATEFUL}, all {@link Operator#process(InvocationContext)}
     * invocations are guaranteed to be done with tuples which have the same partition key.
     * <p>
     * A list of partition field names must be provided during the operator composition.
     *
     * @see KVStore
     */
    PARTITIONED_STATEFUL,

    /**
     * The engine processes all the keys using only a single instance of the operator.
     * <p>
     * Operator must manage its local state using the provided {@link KVStore} implementation provided within
     * the {@link InvocationContext}. Then the engine handles the safety and migrations of the local state.
     *
     * @see KVStore
     */
    STATEFUL
}
