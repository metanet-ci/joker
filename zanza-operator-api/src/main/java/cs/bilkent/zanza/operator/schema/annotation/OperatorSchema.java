package cs.bilkent.zanza.operator.schema.annotation;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import cs.bilkent.zanza.flow.FlowDefinition;
import cs.bilkent.zanza.operator.InvocationContext;
import cs.bilkent.zanza.operator.Operator;
import cs.bilkent.zanza.operator.Tuple;
import cs.bilkent.zanza.operator.schema.runtime.PortRuntimeSchema;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Annotation to define schema of an operator at design time. Schema of an operator is defined separately
 * for each input and output ports separately. For any port, a list of field name and value type pairs are defined. These fields
 * are guaranteed to be included in the {@link Tuple} instances given to the {@link Operator#process(InvocationContext)} in the
 * runtime. {@link Tuple} instances may have any other field that is not given in the corresponding port schema.
 * <p>
 * Field list for a port can be completely defined, partially defined or not-defined at all at design time.
 * See {@link PortSchemaScope}.
 * <p>
 * Port schemas are used while connecting {@link Operator} instances within a {@link FlowDefinition}.
 * See {@link PortRuntimeSchema#isCompatibleWith(PortRuntimeSchema)}.
 */
@Retention( RUNTIME )
@Target( TYPE )
public @interface OperatorSchema
{

    /**
     * Input schemas of an operator. It is not necessary to define schemas for all of the input ports of an operator.
     *
     * @return input schemas of an operator.
     */
    PortSchema[] inputs ();

    /**
     * Output schemas of an operator. It is not necessary to define schemas for all of the output ports of an operator.
     *
     * @return output schemas of an operator.
     */
    PortSchema[] outputs ();

}
