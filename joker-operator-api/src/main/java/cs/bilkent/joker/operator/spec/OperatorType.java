package cs.bilkent.joker.operator.spec;


import cs.bilkent.joker.flow.FlowDef;
import cs.bilkent.joker.operator.InvocationCtx;
import cs.bilkent.joker.operator.Operator;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.kvstore.KVStore;


/**
 * Defines type of an {@link Operator} within {@link OperatorSpec} related to its computation state manipulation aspects.
 *
 * @see OperatorSpec
 * @see Operator
 * @see KVStore
 * @see InvocationCtx
 */
public enum OperatorType
{

    /**
     * Specifies that an operator does not maintain any internal state while processing tuples. Based on this fact,
     * the runtime may create multiple instances of a {@code STATELESS} operator as it sees fit.
     * <p>
     * The engine does not provide any state manipulation capabilities to an operator defined as {@code STATELESS}.
     * No {@link KVStore} implementation can be accessed via {@link InvocationCtx#getKVStore()} for an invocation
     * of a {@code STATELESS} operator.
     * <p>
     * A {@code STATELESS} operator can have at most 1 input port. It can have multiple output ports.
     */
    STATELESS,

    /**
     * Specifies that an operator maintains computation state which can be partitioned by a user-defined key. Operator state must be
     * manipulated using the provided {@link KVStore} implementation which can be accessed via {@link InvocationCtx#getKVStore()}
     * method. It is the runtime's responsibility to maintain lifecycle of the {@link KVStore} objects to simplify development of
     * stateful computations. Operators should not cache the {@link KVStore} object within the operator instance, because the runtime
     * can provide different {@link KVStore} instances for each invocation.
     * <p>
     * Ordering guarantees are given for the tuples of a particular partition key. For instance, if 2 tuples A and B, which are produced
     * in an upstream operator, hit the same partition key, they will be processed as A and B in the {@code PARTITIONED_STATEFUL}
     * downstream operator. On the other hand, there is no ordering guarantee among them if they are tuples of different partition keys.
     * If tuples A and B hit different partition keys, {@code PARTITIONED_STATEFUL} downstream operator can process B first, and then A.
     * <p>
     * For a {@code PARTITIONED_STATEFUL} operator, all {@link Operator#invoke(InvocationCtx)} invocations are
     * guaranteed to be done with tuples which have the same partition key.
     * <p>
     * A list of partition field names must be provided during the operator composition via {@link OperatorDef} and {@link FlowDef}.
     *
     * @see KVStore
     * @see InvocationCtx
     */
    PARTITIONED_STATEFUL,

    /**
     * Specifies that an operator maintains a global computation state. It means that the runtime maintains a single instance of a
     * {@code STATEFUL} operator at any time.
     * <p>
     * An operator implementation must manipulate its computation state using the {@link KVStore} implementation accessed via
     * {@link InvocationCtx#getKVStore()} method. Operators should not cache the {@link KVStore} object within the operator instance,
     * because the runtime can provide different {@link KVStore} instances for each invocation.
     *
     * @see KVStore
     * @see InvocationCtx
     */
    STATEFUL

}
