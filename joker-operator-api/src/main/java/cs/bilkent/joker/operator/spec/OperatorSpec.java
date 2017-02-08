package cs.bilkent.joker.operator.spec;


import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import cs.bilkent.joker.flow.FlowDef;
import static cs.bilkent.joker.flow.Port.DYNAMIC_PORT_COUNT;
import cs.bilkent.joker.operator.Operator;


/**
 * Defines type, input port count and output port count of {@link Operator} implementations. The runtime reads {@link OperatorSpec}
 * annotations to understand behavior of the operators, and to provide necessary capabilities during the execution.
 * <p>
 * {@code OperatorSpec} annotation is mandatory for the operators.
 *
 * @see Operator
 * @see OperatorType
 * @see FlowDef
 */
@Retention( RetentionPolicy.RUNTIME )
@Target( ElementType.TYPE )
public @interface OperatorSpec
{

    /**
     * Specifies how the operator performs its computation
     *
     * @return type of the operator which specifies how the operator performs its computation
     */
    OperatorType type ();

    /**
     * Specifies number of input ports of an operator at design time.
     * If not defined within this annotation, it must be given at runtime while composing the {@link FlowDef}.
     *
     * @return the number of input ports of an operator at design time
     *
     * @see FlowDef
     */
    int inputPortCount () default DYNAMIC_PORT_COUNT;

    /**
     * Returns the number of output ports of an operator at design time.
     * If not defined within this annotation, it must be given at runtime while composing the {@link FlowDef}.
     *
     * @return the number of output ports of an operator at design time
     *
     * @see FlowDef
     */
    int outputPortCount () default DYNAMIC_PORT_COUNT;

}
