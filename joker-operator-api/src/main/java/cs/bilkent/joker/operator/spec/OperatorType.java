package cs.bilkent.joker.operator.spec;


import cs.bilkent.joker.flow.FlowDef;
import cs.bilkent.joker.operator.InvocationContext;
import cs.bilkent.joker.operator.Operator;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.kvstore.KVStore;


/**
 * Defines type of an {@link Operator} within {@link OperatorSpec} related to its computation state manipulation aspects.
 *
 * @see OperatorSpec
 * @see Operator
 * @see KVStore
 * @see InvocationContext
 */
public enum OperatorType
{

    /**
     * Specifies that an operator does not maintain any internal state while processing tuples. Based on this fact, the runtime engine
     * may create multiple instances of a {@code STATELESS} operator as it sees fit.
     * <p>
     * The engine does not provide any state manipulation capabilities to an operator defined as {@code STATELESS}.
     * No {@link KVStore} implementation can be accessed via {@link InvocationContext#getKVStore()} for an invocation
     * of a {@code STATELESS} operator.
     * <p>
     * A {@code STATELESS} operator can have at most 1 input port.
     */
    STATELESS,

    /**
     * Specifies that an operator maintains internal state which can be partitioned by a user-defined key. Based on this fact, the runtime
     * engine can divide the key space into partitions and map these partitions to instances of a {@code PARTITIONED_STATEFUL} operator.
     * <p>
     * Operator state must be manipulated using the provided {@link KVStore} implementation given via {@link InvocationContext#getKVStore()}
     * method. It is the runtime engine's responsibility to maintain lifecycle of the {@link KVStore} objects to simplify development of
     * stateful computations.
     * <p>
     * The engine does not provide any ordering guarantees for the tuples with different partition keys. However,
     * ordering guarantees are given for the tuples of a particular partition key. For instance, if 2 tuples belong to the same
     * partition key, they will be processed due to their creation order. On the other hand, there is no ordering guarantee among them if
     * they are tuples of different partition keys.
     * <p>
     * For an operator defined as {@code PARTITIONED_STATEFUL}, all {@link Operator#invoke(InvocationContext)}
     * invocations are guaranteed to be done with tuples which have the same partition key.
     * <p>
     * A list of partition field names must be provided during the operator composition via {@link OperatorDef} and {@link FlowDef}.
     *
     * @see KVStore
     * @see InvocationContext
     */
    PARTITIONED_STATEFUL,

    /**
     * Specifies that an operator maintains global state, which cannot be partitioned by any key. It means that the runtime engine
     * is going to maintain a single instance of a {@code STATEFUL} operator at any time, even if the operator can be instantiated
     * multiple times, based on scaling decisions of the runtime engine.
     * <p>
     * An operator implementation must manipulate its local state using the {@link KVStore} implementation given via
     * {@link InvocationContext#getKVStore()} method. There will be a singleton {@link KVStore} object that will keep all of operator's
     * state.
     *
     * @see KVStore
     * @see InvocationContext
     */
    STATEFUL

}
