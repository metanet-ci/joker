package cs.bilkent.joker.operator;


import java.util.List;

import cs.bilkent.joker.flow.Port;
import cs.bilkent.joker.operator.kvstore.KVStore;
import cs.bilkent.joker.operator.scheduling.SchedulingStrategy;
import cs.bilkent.joker.operator.spec.OperatorType;


/**
 * Contains necessary objects and information for an invocation of {@link Operator#invoke(InvocationContext)} method.
 */
public interface InvocationContext
{

    /**
     * Returns the tuples available for processing on the given input port index.
     * Once the invocation of {@link Operator#invoke(InvocationContext)} method is completed, these tuples are considered as processed.
     *
     * @param portIndex
     *         the port index to get the input tuples
     *
     * @return the tuples available for processing on the given input port index
     */
    List<Tuple> getInputTuples ( int portIndex );

    /**
     * Returns the tuples available for processing on the default port.
     * Once the invocation of {@link Operator#invoke(InvocationContext)} method is completed, these tuples are considered as processed.
     *
     * @return the tuples available for processing on the default port
     *
     * @see Port#DEFAULT_PORT_INDEX
     */
    default List<Tuple> getInputTuplesByDefaultPort ()
    {
        return getInputTuples( Port.DEFAULT_PORT_INDEX );
    }

    /**
     * Returns the Nth input tuple at the given port index, where the tupleIndex parameter is N, or returns null if there is no tuple
     * at the given tupleIndex
     *
     * @param portIndex
     *         the port index to get the input tuple
     * @param tupleIndex
     *         the tuple index to get the input tuple
     *
     * @return the Nth input tuple at the given port index, where the tupleIndex parameter is N, or returns null if there is no tuple
     * at the given tupleIndex
     */
    Tuple getInputTupleOrNull ( int portIndex, int tupleIndex );

    /**
     * Returns the Nth input tuple at the given port index, where the tupleIndex parameter is N, or fails with
     * {@link IllegalArgumentException} if there is no tuple at the given tupleIndex
     *
     * @param portIndex
     *         the port index to get the input tuple
     * @param tupleIndex
     *         the tuple index to get the input tuple
     *
     * @return the Nth input tuple at the given port index, where the tupleIndex parameter is N, or fails with
     * {@link IllegalArgumentException} if there is no tuple at the given tupleIndex
     *
     * @throws IllegalArgumentException
     *         if there is no tuple exists with the given indices
     */
    default Tuple getInputTupleOrFail ( final int portIndex, final int tupleIndex )
    {
        final Tuple tuple = getInputTupleOrNull( portIndex, tupleIndex );
        if ( tuple != null )
        {
            return tuple;
        }

        throw new IllegalArgumentException( "no tuple exists for input port index " + portIndex + " and tuple index " + tupleIndex );
    }

    /**
     * Returns the number of tuples that are available in the given input port.
     *
     * @param portIndex
     *         port index to get number of tuples
     *
     * @return the number of tuples that are available in the given input port.
     */
    int getInputTupleCount ( int portIndex );

    /**
     * Adds the given tuple to the default output port.
     *
     * @param tuple
     *         tuple to add to the default output port
     *
     * @see Port#DEFAULT_PORT_INDEX
     */
    void output ( Tuple tuple );

    /**
     * Adds the given tuples to the default output port.
     *
     * @param tuples
     *         tuples to add to the default output port
     *
     * @see Port#DEFAULT_PORT_INDEX
     */
    void output ( List<Tuple> tuples );

    /**
     * Adds the given tuple to the given output port.
     *
     * @param portIndex
     *         target output port index
     * @param tuple
     *         tuple to add
     */
    void output ( int portIndex, Tuple tuple );

    /**
     * Adds the given tuples to the given output port.
     *
     * @param portIndex
     *         target output port index
     * @param tuples
     *         tuples to add
     */
    void output ( int portIndex, List<Tuple> tuples );

    /**
     * Returns the reason of a particular {@link Operator#invoke(InvocationContext)} method invocation.
     *
     * @return the reason of a particular {@link Operator#invoke(InvocationContext)} method invocation.
     */
    InvocationReason getReason ();

    /**
     * Indicates that the invocation is done with respect to the {@link SchedulingStrategy} returned from {@link Operator#shutdown()}.
     * If it is {@code false}, it means that the invocation is done although the provided {@link SchedulingStrategy} has not satisfied.
     *
     * @return {@code true} if the invocation is done with respect to the given {@link SchedulingStrategy}, {@code false} otherwise
     */
    default boolean isSuccessfulInvocation ()
    {
        return getReason().isSuccessful();
    }

    /**
     * Indicates that the invocation is done due to a special action in the system. Possible reasons are such that
     * shutdown request may be received by the system or an upstream operator may be completed its run.
     *
     * @return {@code true} if the invocation is done due to a special action in the system.
     */
    default boolean isErroneousInvocation ()
    {
        return getReason().isFailure();
    }

    /**
     * Returns {@code true} if the input port specified with the port index is connected to an upstream operator
     *
     * @param portIndex
     *         to check if the given input port has an upstream operator or not
     *
     * @return {@code true} if the input port specified with the port index is connected to an upstream operator
     */
    boolean isInputPortOpen ( int portIndex );

    /**
     * Returns {@code true} if the input port specified with the port index is not connected to an upstream operator
     *
     * @param portIndex
     *         to check if the given input port has an upstream operator or not
     *
     * @return {@code true} if the input port specified with the port index is not connected to an upstream operator
     */
    default boolean isInputPortClosed ( int portIndex )
    {
        return !isInputPortOpen( portIndex );
    }

    /**
     * Returns the partition key that is present in the {@link Tuple} instances provided as invocation input
     *
     * @return the partition key that is present in the {@link Tuple} instances provided as invocation input
     */
    List<Object> getPartitionKey ();

    /**
     * Returns the {@link KVStore} that can be used for only the current invocation of {@link OperatorType#PARTITIONED_STATEFUL}
     * and {@link OperatorType#STATEFUL} operators.
     * <p>
     * Different {@link KVStore} objects can be given for different invocations. Therefore, {@link KVStore} objects must not be stored
     * as a local field and only the {@link KVStore} object provided by {@link InvocationContext} for the current invocation must be used.
     *
     * @return the {@link KVStore} that can be used within only the particular invocation for only {@link OperatorType#PARTITIONED_STATEFUL}
     * and {@link OperatorType#STATEFUL} operators.
     */
    KVStore getKVStore ();

    /**
     * Indicates the reason for a particular invocation of {@link Operator#invoke(InvocationContext)} method.
     */
    enum InvocationReason
    {

        /**
         * Indicates that the invocation is done with respect to the provided {@link SchedulingStrategy}
         */
        SUCCESS
                {
                    public boolean isSuccessful ()
                    {
                        return true;
                    }
                },

        /**
         * Indicates that the invocation is done before the Engine shuts down. If the operator produces new tuples within the invocation,
         * they will be fed into the next operator in the flow.
         */
        SHUTDOWN
                {
                    public boolean isSuccessful ()
                    {
                        return false;
                    }
                },

        /**
         * Indicates that the invocation is done because some of the input ports have been closed. Because of this, the provided
         * {@link SchedulingStrategy} of the operator may not be satisfied anymore.
         */
        INPUT_PORT_CLOSED
                {
                    public boolean isSuccessful ()
                    {
                        return false;
                    }
                };

        /**
         * Indicates that the invocation is done with respect to the last provided {@link SchedulingStrategy}.
         * If it is false, it means that the invocation is done without the provided {@link SchedulingStrategy} has met
         *
         * @return true if the invocation is done with respect to the last provided {@link SchedulingStrategy}, false otherwise
         */
        abstract boolean isSuccessful ();

        boolean isFailure ()
        {
            return !isSuccessful();
        }

    }

}
