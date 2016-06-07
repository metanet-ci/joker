package cs.bilkent.zanza.operator;


import cs.bilkent.zanza.operator.kvstore.KVStore;
import cs.bilkent.zanza.operator.scheduling.SchedulingStrategy;
import cs.bilkent.zanza.operator.spec.OperatorType;


/**
 * Contains necessary objects and information for an invocation of {@link Operator#invoke(InvocationContext)} method.
 */
public interface InvocationContext
{

    /**
     * Returns the tuples available for being processed by the operator.
     * Once the invocation of {@link Operator#invoke(InvocationContext)} method is completed, these tuples will be considered as processed.
     *
     * @return the tuples available for being processed by the operator
     */
    Tuples getInput ();

    /**
     * Returns the {@link Tuples} object into which the tuples produced by the invocation will be added
     *
     * @return the {@link Tuples} object into which the tuples produced by the invocation will be added
     */
    Tuples getOutput ();

    /**
     * Returns the reason of a particular {@link Operator#invoke(InvocationContext)} method invocation.
     *
     * @return the reason of a particular {@link Operator#invoke(InvocationContext)} method invocation.
     */
    InvocationReason getReason ();

    /**
     * Indicates that the invocation is done with respect to the last provided {@link SchedulingStrategy}.
     * If it is false, it means that the invocation is done without the provided {@link SchedulingStrategy} has met
     *
     * @return true if the invocation is done with respect to the last provided {@link SchedulingStrategy}, false otherwise
     */
    default boolean isSuccessfulInvocation ()
    {
        return getReason().isSuccessful();
    }

    /**
     * Indicates that the invocation is done due to a special action in the system. Possible reasons are such that
     * shutdown request may be received by the system or an upstream operator may be completed its run.
     *
     * @return true if the invocation is done due to a special action in the system.
     */
    default boolean isErroneousInvocation ()
    {
        return getReason().isFailure();
    }

    /**
     * Returns true if the input port specified with the port index is connected to an upstream operator
     *
     * @param portIndex
     *         to check the input port
     *
     * @return true if the input port specified with the port index is connected to an upstream operator
     */
    boolean isInputPortOpen ( int portIndex );

    /**
     * Returns true if the input port specified with the port index is not connected to an upstream operator
     *
     * @param portIndex
     *         to check the input port
     *
     * @return true if the input port specified with the port index is not connected to an upstream operator
     */
    default boolean isInputPortClosed ( int portIndex )
    {
        return !isInputPortClosed( portIndex );
    }

    /**
     * Returns the {@link KVStore} that can be used within only the particular invocation for only {@link OperatorType#PARTITIONED_STATEFUL}
     * and {@link OperatorType#STATEFUL} operators.
     * <p>
     * Different {@link KVStore} objects can be given for different invocations. Therefore, {@link KVStore} objects must not be stored
     * as a local field and only the {@link KVStore} object provided by the {@link InvocationContext} must be used within the invocation.
     *
     * @return the {@link KVStore} that can be used within only the particular invocation for only {@link OperatorType#PARTITIONED_STATEFUL}
     * and {@link OperatorType#STATEFUL} operators.
     */
    KVStore getKVStore ();

    /**
     * Sets a new {@link SchedulingStrategy} that will be used for next invocation of the operator
     *
     * @param schedulingStrategy
     *         a new {@link SchedulingStrategy} that will be used for next invocation of the operator
     */
    void setNextSchedulingStrategy ( SchedulingStrategy schedulingStrategy );

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
                },

        OPERATOR_REQUESTED_SHUTDOWN
                {
                    @Override
                    boolean isSuccessful ()
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
