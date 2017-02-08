package cs.bilkent.joker.operator.schema.annotation;


import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import cs.bilkent.joker.flow.FlowDef;
import cs.bilkent.joker.operator.Operator;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.schema.runtime.PortRuntimeSchema;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;


/**
 * Annotation to define schema of an operator at design time. Schemas are defined separately for each input and output port.
 * For any port, a list of field name and value type pairs are given. The runtime does not verify the existence of the given fields
 * during the execution, and it assumes that {@link Tuple} instances contain the fields specified in the schema.
 * Additionally, {@link Tuple} instances may have any other field that is not given in the corresponding port schema.
 * <p>
 * Field list for a port can be defined at design time and extended at runtime.
 * See {@link PortSchemaScope}.
 * <p>
 * Port schemas are used while connecting {@link Operator} instances within a {@link FlowDef}.
 * See {@link PortRuntimeSchema#isCompatibleWith(PortRuntimeSchema)}.
 */
@Retention( RUNTIME )
@Target( TYPE )
public @interface OperatorSchema
{

    /**
     * Input schemas of an operator.
     *
     * @return input schemas of an operator.
     */
    PortSchema[] inputs () default {};

    /**
     * Output schemas of an operator.
     *
     * @return output schemas of an operator.
     */
    PortSchema[] outputs () default {};

}
