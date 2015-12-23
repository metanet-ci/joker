package cs.bilkent.zanza.operator.schema.annotation;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention( RetentionPolicy.RUNTIME )
public @interface SchemaField
{
    String name ();

    Class<?> type ();

}
