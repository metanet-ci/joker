package cs.bilkent.zanza.operator;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static cs.bilkent.zanza.operator.Port.DYNAMIC_PORT_COUNT;

@Retention( RetentionPolicy.RUNTIME )
@Target( ElementType.TYPE )
public @interface OperatorSpec
{
    OperatorType type ();

    int inputPortCount () default DYNAMIC_PORT_COUNT;

    int outputPortCount () default DYNAMIC_PORT_COUNT;
}
