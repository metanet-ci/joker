package cs.bilkent.zanza.operator.spec;


import cs.bilkent.zanza.operator.InvocationContext;
import cs.bilkent.zanza.operator.Operator;
import cs.bilkent.zanza.operator.kvstore.KVStore;


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
     * The engine may create multiple instances of a {@code STATELESS} operator as it sees fit.
     * <p>
     * The engine does not provide any state manipulation capabilities to an operator defined as {@code STATELESS}.
     * No {@link KVStore} implementation is given to the {@link Operator#invoke(InvocationContext)} for an invocation.
     * <p>
     * A {@code STATELESS} operator can have at most 1 input port.
     */
    STATELESS,

    /**
     * The engine creates multiple instances of the operator and divides the key space into partitions
     * to process each partition using a single instance of the operator.
     * <p>
     * An operator implementation must manipulate its local state using the provided {@link KVStore} implementation
     * provided within the {@link InvocationContext}, which is managed by the engine.
     * <p>
     * The engine does not provide any ordering guarantees for the tuples with different partition keys. However,
     * ordering guarantees are given for the tuples of a particular partition key.
     * <p>
     * For an operator defined as {@code PARTITIONED_STATEFUL}, all {@link Operator#invoke(InvocationContext)}
     * invocations are guaranteed to be done with tuples which have the same partition key.
     * <p>
     * A list of partition field names must be provided during the operator composition.
     *
     * @see KVStore
     */
    PARTITIONED_STATEFUL,

    /**
     * The engine creates a single instance of an operator defined as {@code STATEFUL}
     * <p>
     * An operator implementation must manipulate its local state using the provided {@link KVStore} implementation
     * provided within the {@link InvocationContext}, which is managed by the engine. There will be a singleton {@link KVStore}
     * object created for the operator.
     *
     * @see KVStore
     */
    STATEFUL
}
