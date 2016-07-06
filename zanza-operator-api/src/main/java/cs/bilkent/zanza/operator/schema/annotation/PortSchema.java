package cs.bilkent.zanza.operator.schema.annotation;


import java.lang.annotation.Retention;

import cs.bilkent.zanza.flow.FlowDefBuilder;
import static java.lang.annotation.RetentionPolicy.RUNTIME;


/**
 * Annotation to define schema of an input / output port of an operator at design time.
 */
@Retention( RUNTIME )
public @interface PortSchema
{

    /**
     * Port index of the defined schema
     *
     * @return port index of the defined schema
     */
    int portIndex ();

    /**
     * Scope of the schema. All field names that are guaranteed to be present in the runtime maybe defined in design time, or
     * list of the field names can be extended while composing the flow via {@link FlowDefBuilder}
     *
     * @return scope of the schema
     */
    PortSchemaScope scope ();

    /**
     * Array of the fields defined in the port schema. For each field, a name and a type is defined.
     *
     * @return array of the fields defined in the port schema
     */
    SchemaField[] fields ();

}
