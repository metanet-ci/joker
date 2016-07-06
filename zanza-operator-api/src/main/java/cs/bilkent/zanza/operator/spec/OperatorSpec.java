package cs.bilkent.zanza.operator.spec;


import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import cs.bilkent.zanza.flow.FlowDef;
import static cs.bilkent.zanza.flow.Port.DYNAMIC_PORT_COUNT;
import cs.bilkent.zanza.operator.Operator;


/**
 * Defines specifications of the {@link Operator} that will be used by the Engine to create,
 * instantiate an operator, and use it for processing the tuples.
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
     * Returns type of the operator that defines its state-related characteristics
     *
     * @return type of the operator that defines its state-related characteristics
     */
    OperatorType type ();

    /**
     * Returns the number of input ports that an operator uses during processing tuples.
     * If not defined within the operator class, it must be provided while composing the {@link FlowDef}
     *
     * @return the number of input ports that an operator uses during processing tuples
     *
     * @see FlowDef
     */
    int inputPortCount () default DYNAMIC_PORT_COUNT;

    /**
     * Returns the number of output ports that an operator produces tuples during the processing.
     * If not defined within the operator class, it must be provided while composing the {@link FlowDef}
     *
     * @return the number of output ports that an operator produces tuples during the processing
     *
     * @see FlowDef
     */
    int outputPortCount () default DYNAMIC_PORT_COUNT;

}
