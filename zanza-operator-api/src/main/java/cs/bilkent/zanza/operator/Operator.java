package cs.bilkent.zanza.operator;

import cs.bilkent.zanza.operator.flow.FlowDefinition;
import cs.bilkent.zanza.operator.kvstore.KVStore;
import cs.bilkent.zanza.operator.scheduling.SchedulingStrategy;

/**
 * {@code Operator} is the main component that is responsible for producing or processing tuples.
 * An operator implementation is provided to {@link FlowDefinition}
 * with its configuration and its life-cycle is managed by the Engine afterwards.
 *
 * @see Tuple
 */
public interface Operator
{

    /**
     * Invoked after an operator is created by the Engine and before the processing starts.
     * All the information and objects that can be used during lifetime of an operator is provided with the
     * {@link OperatorContext} instance. Operators can save references of the objects returned by the
     * {@link OperatorContext}, i.e. {@link OperatorConfig}, {@link KVStore} e.g. into their class fields.
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
     * @see OperatorContext
     */
    SchedulingStrategy init ( final OperatorContext context );

    /**
     * Invoked to process incoming tuples sent by incoming connections of an operator and to produce tuples that will be
     * dispatched to output connections of the operator.
     * <p>
     * The tuples sent by all incoming ports are contained within the {@link PortsToTuples} object. This object is a read-only
     * object such that any modifications within the process method can cause inconsistent behavior in the system. Additionally,
     * the {@link Tuple} objects contained within the {@link PortsToTuples} should not be modified.
     * <p>
     * {@link PortsToTuples} object or the tuples it contains can be returned within the {@link ProcessingResult} object as output.
     * <p>
     * Invocation can be done due to the {@link SchedulingStrategy} of the operator or a system event that requires immediate
     * processing of the remaining tuples. Status of the invocation can be queried via {@link InvocationReason#isSuccessful()}.
     * If it is true, it means that invocation is done due to the {@link SchedulingStrategy} and operator can continue to
     * operate normally by processing tuples, updating its state etc. If it is false, there will be no more invocations and
     * all provided tuples must be processed.
     * <p>
     * A {@link SchedulingStrategy} must be returned within the {@link ProcessingResult} in order to specify the scheduling condition
     * of the operator for the next invocation. If the current invocation is not a successful invocation (i.e. {@link InvocationReason#isSuccessful()}),
     * the next invocation is not guaranteed.
     * <p>
     * If type of the operator is {@link OperatorType#PARTITIONED_STATEFUL}, then partition keys of the tuples can be queried via
     * {@link Tuple#getPartitionKey()} method. Additionally, partition keys of the produced tuples can be set using
     * {@link Tuple#copyPartitionTo(Tuple)} method.
     *
     * @param portsToTuples
     *         tuples produced by the incoming connections of the operator.
     * @param reason
     *         whether the scheduling conditions are satisfied or a system event occurred.
     *
     * @return a {@link ProcessingResult} object that contains the produced tuples that will be sent to output connections and a new
     * {@link SchedulingStrategy} that will be used for the next invocation.
     *
     * @see Tuple
     * @see PortsToTuples
     * @see InvocationReason
     */
    ProcessingResult process ( PortsToTuples portsToTuples, InvocationReason reason );

    /**
     * Invoked after the Engine terminates processing of an operator. All the resources allocated within the
     * {@link Operator#init(OperatorContext)} method must be closed.
     * <p>
     * This method is not guaranteed to be invoked during crashes.
     */
    default void destroy ()
    {

    }

}
