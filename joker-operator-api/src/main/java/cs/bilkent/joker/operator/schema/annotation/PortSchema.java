package cs.bilkent.joker.operator.schema.annotation;


import java.lang.annotation.Retention;

import static java.lang.annotation.RetentionPolicy.RUNTIME;


/**
 * Annotation to define the schema of an input port or output port of an operator at design time.
 */
@Retention( RUNTIME )
public @interface PortSchema
{

    /**
     * Port index of the schema
     *
     * @return port index of the schema
     */
    int portIndex ();

    /**
     * Scope of the schema. All fields can be explicitly defined in design time,
     * or they can be extended while composing the flow in the runtime.
     *
     * @return scope of the schema
     */
    PortSchemaScope scope ();

    /**
     * Array of the fields defined in the port schema. For each field, name and type are required.
     *
     * @return array of the fields defined for the port
     */
    SchemaField[] fields ();

}
