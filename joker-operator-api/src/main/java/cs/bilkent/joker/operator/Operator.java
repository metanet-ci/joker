package cs.bilkent.joker.operator;


import cs.bilkent.joker.flow.FlowDef;
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
 * {@code Operator} is the main abstraction which is defined for performing user computation.
 * An operator implementation is provided to {@link FlowDef} with various configuration options.
 * <p>
 * {@link OperatorSpec} annotation is used to define type and port counts of the operator. Type of an operator reflects to what extend
 * an operator implementation performs stateful computation.
 * Please see {@link OperatorSpec} and {@link OperatorType} for more information.
 * <p>
 * Operators can have schemas for their input output ports. A port schema is basically a list of field names along with their types.
 * Design time schema of an operator can be specified with {@link OperatorSchema} annotation, and it can be extended during runtime.
 * Please see {@link OperatorSchema}, {@link PortSchema} and {@link OperatorRuntimeSchema}.
 * <p>
 * Based on its type, multiple instances of an operator implementation can be created at the same time, or at different times
 * by the runtime engine. It is guaranteed that each operator instance is executed by a single thread, which provides a very simple
 * threading model to operator developers. {@link Operator#init(InitializationContext)} and {@link Operator#shutdown()} methods are defined
 * to allow developers to initialize and clean up internal state of their operators. The runtime engine guarantees that these lifecycle
 * methods are going to be invoked properly every time an operator is instantiated.
 *
 * @see InitializationContext
 * @see InvocationContext
 * @see Tuples
 * @see Tuple
 */
public interface Operator
{

    /**
     * Invoked after an operator instance is created by the runtime engine, and before the processing starts. All the information,
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
     * return its {@link SchedulingStrategy} deterministically as the initialization method may be invoked multiple times.
     * Please see {@link ScheduleWhenAvailable} and {@link ScheduleWhenTuplesAvailable}
     *
     * @param context
     *         contains various information that can be used during lifetime of an operator.
     *
     * @return a {@link SchedulingStrategy} that will be used for scheduling the operator.
     */
    SchedulingStrategy init ( InitializationContext context );

    /**
     * Invoked to process input tuples, and to produce output tuples by an operator implementation. It is going to be invoked by the
     * runtime engine when the {@link SchedulingStrategy} given by the operator is satisfied.
     * <p>
     * All the necessary information, such as input tuples, invocation reason, etc., about a particular invocation of the
     * {@link #invoke(InvocationContext)} method is given in the {@link InvocationContext} object. Please keep in mind that
     * the objects accessed via {@link InvocationContext} should be shared between multiple invocations. An operator implementation
     * should use those objects only for that particular invocation.
     * <p>
     * Tuples sent to the incoming ports of an operator are contained within the {@link Tuples} object which is obtained via
     * {@link InvocationContext#getInput()} method. Similarly, tuples produced during an invocation can be passed to the downstream by
     * using the object obtained via {@link InvocationContext#getOutput()} method.
     * <p>
     * {@link Tuples} object in the {@link InvocationContext} or the {@link Tuple} objects it contains can be returned
     * within the {@link Tuples} retrieved via {@link InvocationContext#getOutput()} object as output.
     * <p>
     * Invocation can be done due to the {@link SchedulingStrategy} of the operator or a system event that requires immediate
     * processing of the remaining tuples. Status of the invocation can be queried via {@link InvocationReason#isSuccessful()}.
     * If it is {@code true}, it means that the invocation is done due to the given {@link SchedulingStrategy},
     * and operator can continue to operate normally by processing tuples, updating its state, producing new tuples etc.
     * If it is {@code false}, there will be no more invocations and all of the tuples provided with the {@link InvocationContext}
     * must be processed. If the current invocation is not a successful invocation (i.e. {@link InvocationReason#isSuccessful()}),
     * next invocation is not guaranteed.
     * <p>
     * If type of the operator is {@link OperatorType#PARTITIONED_STATEFUL}, each invocation is going to belong to the same partition key
     * value, and each input tuple have the same partition key.
     * <p>
     * If type of the operator is {@link OperatorType#PARTITIONED_STATEFUL} or {@link OperatorType#STATEFUL}, a {@link KVStore}
     * implementation is provided with the {@link InvocationContext#getKVStore()} method. Additionally, the runtime engine provides
     * an isolates {@link KVStore} object for each partition key. Stateful computations must be done via {@link KVStore} objects to
     * make operators utilize state management and scalability capabilities of the runtime engine.
     * <p>
     * The computation model of Joker operators are quite similar to the actor-based concurrency. Following rules should be obeyed
     * to perform tuple processing in a correct and thread-safe manner:
     * <ul>
     * <li>{@link Tuples} object return by {@link InvocationContext#getInput()} and {@link Tuple} objects contained by it should be
     * considered final and should not be modified</li>
     * <li>{@link Tuple} objects added to the {@link Tuples} object accessed via {@link InvocationContext#getOutput()} should not be
     * modified after the invocation is completed.</li>
     * <li>All mutable state must be kept using the objects which are not shared by any other operator in the runtime.</li>
     * </ul>
     *
     * @param invocationContext
     *         all the necessary information about a particular invocation of the method, such as input tuples, invocation reason etc.
     *
     * @see Tuple
     * @see InvocationContext
     */
    void invoke ( InvocationContext invocationContext );

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
