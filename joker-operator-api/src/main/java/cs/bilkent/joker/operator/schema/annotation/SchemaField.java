package cs.bilkent.joker.operator.schema.annotation;


import java.lang.annotation.Retention;

import cs.bilkent.joker.operator.Tuple;
import static java.lang.annotation.RetentionPolicy.RUNTIME;


/**
 * Annotation to define a field of a {@link Tuple} in the design time
 */
@Retention( RUNTIME )
public @interface SchemaField
{
    /**
     * Name of the field
     *
     * @return name of the field
     */
    String name ();

    /**
     * Type of the value of the field
     *
     * @return type of the value of the field
     */
    Class<?> type ();

}
