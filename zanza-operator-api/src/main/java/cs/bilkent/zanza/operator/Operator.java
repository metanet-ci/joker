package cs.bilkent.zanza.operator;

import cs.bilkent.zanza.operator.InvocationContext.InvocationReason;
import cs.bilkent.zanza.operator.flow.FlowDefinition;

/**
 * {@code Operator} is the main component that is responsible for producing or processing tuples.
 * An operator implementation is provided to {@link FlowDefinition}
 * with its configuration and its life-cycle is managed by the Engine afterwards.
 *
 * State-related characteristics of a user-defined operator must be annotated with the {@link OperatorSpec}
 * annotation.
 *
 * TODO TALK ABOUT THREAD SAFETY AND OPERATOR EXECUTION
 *
 * @see OperatorSpec
 * @see InitializationContext
 * @see InvocationContext
 * @see InvocationResult
 * @see PortsToTuples
 * @see Tuple
 */
public interface Operator
{

    /**
     * Invoked after an operator is created by the Engine and before the processing starts.
     * All the information and objects that can be used during lifetime of an operator is provided with the
     * {@link InitializationContext} instance. Operators can save references of the objects returned by the
     * {@link InitializationContext}, i.e. {@link OperatorConfig}, e.g. into their class fields.
     * <p>
     * An operator can initialize its internal state within this method. For instance, it can allocated resources,
     * create files, connect to some external services etc.
     * <p>
     * Operator must return a {@link SchedulingStrategy} that will be used for scheduling the operator for the first time.
     *
     * @param context
     *         contains all of the objects that can be used during lifetime of an operator.
     *
     * @return a {@link SchedulingStrategy} that will be used for scheduling the operator for the first time.
     *
     * @see SchedulingStrategy
     * @see InitializationContext
     */
    SchedulingStrategy init ( final InitializationContext context );

    /**
     * Invoked to process incoming tuples sent to incoming connections of an operator and to produce tuples that will be
     * dispatched to output connections of the operator.
     * <p>
     * <p>
     * All the necessary information, such as input tuples, invocation reason, etc., about a particular invocation of the
     * {@link #process(InvocationContext)} method is given in the {@link InvocationContext} object.
     * <p>
     * The tuples sent by all incoming ports are contained within the {@link PortsToTuples} object that can be obtained via
     * {@link InvocationContext#getTuples()}. This object is a read-only object such that any modifications within the process
     * method can cause inconsistent behavior in the system. Additionally, the {@link Tuple} objects contained within the
     * {@link PortsToTuples} should not be modified.
     * <p>
     * {@link PortsToTuples} object in the {@link InvocationContext} or the tuples it contains can be returned within the
     * {@link InvocationResult} object as output.
     * <p>
     * Invocation can be done due to the {@link SchedulingStrategy} of the operator or a system event that requires immediate
     * processing of the remaining tuples. Status of the invocation can be queried via {@link InvocationReason#isSuccessful()}.
     * If it is true, it means that invocation is done due to the given {@link SchedulingStrategy} and operator can continue to
     * operate normally by processing tuples, updating its state, producing new tuples etc. If it is false, there will be no more
     * invocations and all of the tuples provided with the {@link InvocationContext} must be processed.
     * <p>
     * A {@link SchedulingStrategy} must be returned within the {@link InvocationResult} in order to specify the scheduling
     * condition of the operator for the next invocation. If the current invocation is not a successful invocation
     * (i.e. {@link InvocationReason#isSuccessful()}), the next invocation is not guaranteed.
     * <p>
     * If type of the operator is {@link OperatorType#PARTITIONED_STATEFUL}, then all invocations are guaranteed to be done with
     * tuples which have the same partition key. Partition keys of the tuples can be queried via {@link Tuple#getPartitionKey()}
     * method. Additionally, partition keys of the produced tuples can be set using {@link Tuple#copyPartitionTo(Tuple)} method.
     * <p>
     * If type of the operator is {@link OperatorType#PARTITIONED_STATEFUL} or {@link OperatorType#STATEFUL}, a {@link KVStore}
     * implementation is provided with the {@link InvocationContext#getKVStore()} method. Additionally, the Engine isolates
     * the data in the {@link KVStore} manipulated by the invocations done for different partition keys from each other. Therefore,
     * If an operator puts an object into the {@link KVStore} using the same {@link KVStore} object key for different partition keys,
     * there will be 2 different objects in the {@link KVStore}, of which each one of them are put for a particular partition key.
     *
     * @param invocationContext
     *         all the necessary information about a particular invocation of the method, such as input tuples, invocation reason etc.
     *
     * @return a {@link InvocationResult} object that contains the produced tuples that will be sent to output connections and a new
     * {@link SchedulingStrategy} that will be used for the next invocation.
     *
     * @see Tuple
     * @see InvocationContext
     */
    InvocationResult process ( InvocationContext invocationContext );

    /**
     * Invoked after the Engine terminates processing of an operator. All the resources allocated within the
     * {@link Operator#init(InitializationContext)} method must be closed.
     * <p>
     * This method is not guaranteed to be invoked during crashes.
     */
    default void destroy ()
    {

    }

}
