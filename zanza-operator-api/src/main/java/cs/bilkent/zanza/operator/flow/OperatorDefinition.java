package cs.bilkent.zanza.operator.flow;


import static com.google.common.base.Preconditions.checkNotNull;
import cs.bilkent.zanza.operator.Operator;
import cs.bilkent.zanza.operator.OperatorConfig;


public class OperatorDefinition
{
    public final String id;

    public final Class<? extends Operator> clazz;

    public final OperatorConfig config;

    public OperatorDefinition ( final String id, final Class<? extends Operator> clazz, final OperatorConfig config )
    {
        checkNotNull( id, "id can't be null" );
        checkNotNull( clazz, "clazz can't be null" );
        this.id = id;
        this.clazz = clazz;
        this.config = config;
    }
}
