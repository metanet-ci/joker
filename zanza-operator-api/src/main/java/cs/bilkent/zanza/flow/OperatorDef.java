package cs.bilkent.zanza.flow;


import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.zanza.operator.Operator;
import cs.bilkent.zanza.operator.OperatorConfig;
import cs.bilkent.zanza.operator.schema.runtime.OperatorRuntimeSchema;
import cs.bilkent.zanza.operator.spec.OperatorType;
import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;

// TODO MAKE FINAL
public class OperatorDef
{
    private final String id;

    private final Class<? extends Operator> clazz;

    private final OperatorType type;

    private final int inputPortCount;

    private final int outputPortCount;

    private final OperatorRuntimeSchema schema;

    private final OperatorConfig config;

    private final List<String> partitionFieldNames;

    public OperatorDef ( final String id,
                         final Class<? extends Operator> clazz,
                         final OperatorType type,
                         final int inputPortCount,
                         final int outputPortCount,
                         final OperatorRuntimeSchema schema,
                         final OperatorConfig config,
                         final List<String> partitionFieldNames )
    {
        checkArgument( id != null, "id can't be null" );
        checkArgument( clazz != null, "clazz can't be null" );
        checkArgument( type != null, "type can't be null" );
        checkArgument( inputPortCount >= 0, "input port count must be non-negative" );
        checkArgument( outputPortCount >= 0, "output port count must be non-negative" );
        checkArgument( schema != null, "schema can't be null" );
        checkArgument( config != null, "config can't be null" );
        this.id = id;
        this.clazz = clazz;
        this.type = type;
        this.inputPortCount = inputPortCount;
        this.outputPortCount = outputPortCount;
        this.schema = schema;
        this.config = config;
        this.partitionFieldNames = partitionFieldNames != null ? unmodifiableList( new ArrayList<>( partitionFieldNames ) ) : emptyList();
    }

    public Operator createOperator () throws IllegalAccessException, InstantiationException
    {
        return clazz.newInstance();
    }

    public String id ()
    {
        return id;
    }

    public Class<? extends Operator> operatorClazz ()
    {
        return clazz;
    }

    public OperatorType operatorType ()
    {
        return type;
    }

    public int inputPortCount ()
    {
        return inputPortCount;
    }

    public int outputPortCount ()
    {
        return outputPortCount;
    }

    public OperatorRuntimeSchema schema ()
    {
        return schema;
    }

    public OperatorConfig config ()
    {
        return config;
    }

    public List<String> partitionFieldNames ()
    {
        return partitionFieldNames;
    }

    @Override
    public boolean equals ( final Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        final OperatorDef that = (OperatorDef) o;

        return id.equals( that.id );

    }

    @Override
    public int hashCode ()
    {
        return id.hashCode();
    }

    @Override
    public String toString ()
    {
        return "OperatorDef{" + "id='" + id + '\'' + ", clazz=" + clazz + ", type=" + type + ", inputPortCount=" + inputPortCount
               + ", outputPortCount=" + outputPortCount + ", schema=" + schema + ", config=" + config + ", partitionFieldNames="
               + partitionFieldNames + '}';
    }

}
