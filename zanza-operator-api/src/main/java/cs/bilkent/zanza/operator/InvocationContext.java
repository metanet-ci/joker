package cs.bilkent.zanza.operator;

/**
 * Contains necessary objects and information for an invocation of {@link Operator#process(InvocationContext)} method.
 */
public interface InvocationContext
{

    /**
     * Returns the tuples available for being processed by the operator.
     * Once the invocation of {@link Operator#process(InvocationContext)} method is completed, these tuples will be considered as processed.
     *
     * @return the tuples available for being processed by the operator
     */
    PortsToTuples getInputTuples ();

    /**
     * Returns the reason of a particular {@link Operator#process(InvocationContext)} method invocation.
     *
     * @return the reason of a particular {@link Operator#process(InvocationContext)} method invocation.
     */
    InvocationReason getReason ();

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
     * Indicates the reason for a particular invocation of {@link Operator#process(InvocationContext)} method.
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

        /**
         * Indicates that the invocation is done because some of the output ports have been closed. Because of this, new tuples produced
         * by the operator will not be fed into the next operator in the flow.
         */
        OUTPUT_PORT_CLOSED
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
    }

}
