package cs.bilkent.joker.operator.kvstore;


import cs.bilkent.joker.operator.Fields;
import cs.bilkent.joker.operator.spec.OperatorType;

/**
 * Maintains operator states for stateful computation.
 * <p>
 * IMPORTANT:
 * Please do not modify an object after it is to the {@code KVStore} during operator invocation.
 * Please do not modify an object instance if it is added to the {@code KVStore} for different partition keys during invocations of a
 * {@link OperatorType#PARTITIONED_STATEFUL} operator.
 */
public interface KVStore extends Fields<Object>
{

    @Override
    default void clear ()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    default int size ()
    {
        throw new UnsupportedOperationException();
    }

}
