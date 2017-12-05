package cs.bilkent.joker.operator;


import cs.bilkent.joker.flow.FlowDef;
import cs.bilkent.joker.flow.FlowDefBuilder;
import cs.bilkent.joker.operator.InvocationContext.InvocationReason;
import cs.bilkent.joker.operator.kvstore.KVStore;
import cs.bilkent.joker.operator.scheduling.ScheduleWhenAvailable;
import cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable;
import cs.bilkent.joker.operator.scheduling.SchedulingStrategy;
import cs.bilkent.joker.operator.schema.annotation.OperatorSchema;
import cs.bilkent.joker.operator.schema.annotation.PortSchema;
import cs.bilkent.joker.operator.schema.runtime.OperatorRuntimeSchema;
import cs.bilkent.joker.operator.schema.runtime.TupleSchema;
import cs.bilkent.joker.operator.spec.OperatorSpec;
import cs.bilkent.joker.operator.spec.OperatorType;


/**
 * {@code Operator} is the main abstraction which is defined for performing computation.
 * An operator implementation is provided to {@link FlowDef} with various configuration options.
 * Please see {@link OperatorDefBuilder} and {@link FlowDefBuilder}.
 * <p>
 * {@link OperatorSpec} annotation is used to define type and port counts of the operator. Type of an operator reflects to what extend
 * an operator implementation performs stateful computation. Please see {@link OperatorSpec} and {@link OperatorType} for more information.
 * <p>
 * Operators can have schemas for their input output ports. A port schema is basically a list of field names along with their types.
 * Design time schema of an operator can be specified with {@link OperatorSchema} annotation, and it can be extended during runtime.
 * Operator schemas are used to make transparent and safe auto-scaling decisions during the execution.
 * Please see {@link OperatorSchema}, {@link PortSchema} and {@link OperatorRuntimeSchema}.
 * <p>
 * Based on its type, multiple instances of an operator implementation can exist in the runtime. It is guaranteed that
 * each operator instance is executed by a single thread, which provides a very simple threading model to operator developers.
 * <p></p>
 * {@link Operator#init(InitializationContext)} and {@link Operator#shutdown()} methods are defined to allow developers to
 * initialize and clean up internal state of their operators. The runtime engine guarantees that these lifecycle methods are
 * going to be invoked properly every time an operator is instantiated.
 *
 * @see InitializationContext
 * @see InvocationContext
 * @see Tuples
 * @see Tuple
 */
public interface Operator
{

    /**
     * Invoked after an operator instance is created by the runtime, and before the processing starts. All the information,
     * and objects that can be used during lifetime of an operator is provided via the {@link InitializationContext} interface.
     * Operators can save references of the objects returned by the {@link InitializationContext}, i.e. {@link OperatorConfig},
     * {@link TupleSchema}, e.g. into their class fields.
     * <p>
     * An operator can initialize its internal state within this method. For instance, it can allocated resources,
     * create files, connect to some external services etc. Please keep in mind that {@link Operator#init(InitializationContext)} method
     * is going to be invoked for every instance of a given operator implementation.
     * Please see {@link InitializationContext} for details.
     * <p>
     * Operator must return a {@link SchedulingStrategy} that will be used for scheduling the operator for the first time. It should
     * return its {@link SchedulingStrategy} deterministically as the initialization method may be invoked multiple times during the
     * execution.
     * Please see {@link ScheduleWhenAvailable} and {@link ScheduleWhenTuplesAvailable}
     *
     * @param ctx
     *         contains various information that can be used during lifetime of an operator.
     *
     * @return a {@link SchedulingStrategy} that will be used for scheduling the operator.
     */
    SchedulingStrategy init ( InitializationContext ctx );

    /**
     * Invoked to process input tuples and produce output tuples by an operator implementation. It is going to be invoked by the
     * runtime when the {@link SchedulingStrategy} of the operator is satisfied, or an exceptional situation occurs.
     * <p>
     * All the necessary objects, such as input tuples, invocation reason, etc., about a particular invocation of the
     * {@link #invoke(InvocationContext)} method is given in the {@link InvocationContext} object. Please keep in mind that
     * the objects accessed via {@link InvocationContext} should not be cached by the operator between multiple invocations, and should be
     * used only within the current invocation.
     * <p>
     * Tuples sent to the incoming ports of an operator are accessed via the methods on {@link InvocationContext}.
     * Similarly, tuples produced during an invocation can be passed to the downstream by using the methods on {@link InvocationContext}.
     * <p>
     * Invocation can be done due to the {@link SchedulingStrategy} of the operator or a system event that requires immediate
     * processing of the remaining tuples. Status of the invocation can be queried via {@link InvocationReason#isSuccessful()}.
     * If it is {@code true}, it means that the invocation is done with respect to the given {@link SchedulingStrategy},
     * and operator can continue to operate normally by processing tuples, updating its state, producing new tuples etc.
     * If it is {@code false}, there will be no more invocations and all of the tuples provided with the {@link InvocationContext}
     * must be processed. If the current invocation is not a successful invocation (i.e. {@link InvocationReason#isSuccessful()}),
     * next invocation is not guaranteed.
     * <p>
     * If type of the operator is {@link OperatorType#PARTITIONED_STATEFUL}, invocations are done on a partition key basis and
     * each input tuple of a particular invocation have the same partition key.
     * <p>
     * If type of the operator is {@link OperatorType#PARTITIONED_STATEFUL} or {@link OperatorType#STATEFUL}, a {@link KVStore}
     * implementation is provided with the {@link InvocationContext#getKVStore()} method. Additionally, the runtime provides
     * an isolated {@link KVStore} object for each partition key. Stateful computations must be done via {@link KVStore} objects so that
     * the runtime will be able to auto-scale stateful operators in a safe and transparent manner.
     * <p>
     * The computation model of Joker operators are quite similar to the actor-based concurrency. Following rules should be obeyed
     * to perform tuple processing in a correct and thread-safe manner:
     * <ul>
     * <li>{@link Tuple} objects returned by input methods of {@link InvocationContext} should be considered final and should not be
     * modified. It is because the same {@link Tuple} objects can be processed by other operators in the flow.</li>
     * <li>{@link Tuple} objects added to the {@link InvocationContext} via output methods should not be modified.</li>
     * </ul>
     *
     * @param ctx
     *         all the necessary information about a particular invocation of the method, such as input tuples, invocation reason etc.
     *
     * @see Tuple
     * @see InvocationContext
     */
    void invoke ( InvocationContext ctx );

    /**
     * Invoked after the runtime engine completes invocation of an operator instance. All the resources allocated within the
     * {@link Operator#init(InitializationContext)} method must be closed.
     * <p>
     * This method is not guaranteed to be invoked during crashes.
     */
    default void shutdown ()
    {

    }

}
