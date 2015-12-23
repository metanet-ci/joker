package cs.bilkent.zanza.operator.schema.annotation;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention( RetentionPolicy.RUNTIME )
public @interface PortSchema
{

    int portIndex ();

    PortSchemaScope scope ();

    SchemaField[] fields ();

}
