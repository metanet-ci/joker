package cs.bilkent.joker.operator;


import cs.bilkent.joker.flow.FlowDef;
import cs.bilkent.joker.operator.InvocationContext.InvocationReason;
import cs.bilkent.joker.operator.kvstore.KVStore;
import cs.bilkent.joker.operator.scheduling.SchedulingStrategy;
import cs.bilkent.joker.operator.schema.annotation.OperatorSchema;
import cs.bilkent.joker.operator.spec.OperatorSpec;
import cs.bilkent.joker.operator.spec.OperatorType;


/**
 * {@code Operator} is the main component that is responsible for producing or processing tuples.
 * An operator implementation is provided to {@link FlowDef}
 * with its configuration and its life-cycle is managed by the Engine afterwards.
 * <p>
 * State-related characteristics of a user-defined operator must be specified with {@link OperatorSpec} annotation.
 * A design time schema of an operator can be specified with {@link OperatorSchema} annotation.
 * <p>
 * TODO TALK ABOUT THREAD SAFETY AND OPERATOR EXECUTION
 *
 * @see OperatorSpec
 * @see OperatorSchema
 * @see InitializationContext
 * @see InvocationContext
 * @see Tuples
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
    SchedulingStrategy init ( InitializationContext context );

    /**
     * Invoked to process incoming tuples sent to incoming connections of an operator and to produce tuples that will be
     * dispatched to output connections of the operator.
     * <p>
     * <p>
     * All the necessary information, such as input tuples, invocation reason, etc., about a particular invocation of the
     * {@link #invoke(InvocationContext)} method is given in the {@link InvocationContext} object. Additionally, output tuples and
     * new scheduling strategy is also set via the {@link InvocationContext} object.
     * <p>
     * The tuples sent by all incoming ports are contained within the {@link Tuples} object that can be obtained via
     * {@link InvocationContext#getInput()}. This object is a read-only object such that any modifications within the invoke
     * method can cause inconsistent behavior in the system. Additionally, the {@link Tuple} objects contained within the
     * {@link Tuples} should not be modified.
     * <p>
     * {@link Tuples} object in the {@link InvocationContext} or the {@link Tuple} objects it contains can be returned
     * within the {@link Tuples} retrieved via {@link InvocationContext#getOutput()} object as output.
     * <p>
     * Invocation can be done due to the {@link SchedulingStrategy} of the operator or a system event that requires immediate
     * processing of the remaining tuples. Status of the invocation can be queried via {@link InvocationReason#isSuccessful()}.
     * If it is true, it means that invocation is done due to the given {@link SchedulingStrategy} and operator can continue to
     * operate normally by processing tuples, updating its state, producing new tuples etc. If it is false, there will be no more
     * invocations and all of the tuples provided with the {@link InvocationContext} must be processed.
     * If the current invocation is not a successful invocation (i.e. {@link InvocationReason#isSuccessful()}),
     * next invocation is not guaranteed.
     * <p>
     * The {@link SchedulingStrategy} object provided via {@link Operator#init(InitializationContext)} is used for further invocations.
     * <p>
     * If type of the operator is {@link OperatorType#PARTITIONED_STATEFUL}, then all invocations are guaranteed to be done with
     * {@link Tuple} objects that have the same partition key.
     * <p>
     * If type of the operator is {@link OperatorType#PARTITIONED_STATEFUL} or {@link OperatorType#STATEFUL}, a {@link KVStore}
     * implementation is provided with the {@link InvocationContext#getKVStore()} method. Additionally, the Engine isolates
     * the data in the {@link KVStore} manipulated by the invocations done for different partition keys from each other. Therefore,
     * If an operator puts an object into the {@link KVStore} using the same {@link KVStore} object key for different partition keys,
     * there will be 2 different objects in the {@link KVStore}, of which each one of them are put for a particular partition key.
     * <p>
     *
     * @param invocationContext
     *         all the necessary information about a particular invocation of the method, such as input tuples, invocation reason etc.
     *
     * @see Tuple
     * @see InvocationContext
     */
    void invoke ( InvocationContext invocationContext );

    /**
     * Invoked after the Engine terminates processing of an operator. All the resources allocated within the
     * {@link Operator#init(InitializationContext)} method must be closed.
     * <p>
     * This method is not guaranteed to be invoked during crashes.
     */
    default void shutdown ()
    {

    }

}
