package cs.bilkent.zanza.flow;


import static com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.zanza.operator.Operator;
import cs.bilkent.zanza.operator.OperatorConfig;
import cs.bilkent.zanza.operator.PartitionKeyExtractor;
import cs.bilkent.zanza.operator.schema.runtime.OperatorRuntimeSchema;
import cs.bilkent.zanza.operator.spec.OperatorType;


public class OperatorDefinition
{
    public final String id;

    public final Class<? extends Operator> clazz;

    public final OperatorType type;

    public final int inputPortCount;

    public final int outputPortCount;

    public final OperatorRuntimeSchema schema;

    public final OperatorConfig config;

    public final PartitionKeyExtractor partitionKeyExtractor;

    public OperatorDefinition ( final String id,
                                final Class<? extends Operator> clazz,
                                final OperatorType type,
                                final int inputPortCount,
                                final int outputPortCount,
                                final OperatorRuntimeSchema schema,
                                final OperatorConfig config,
                                final PartitionKeyExtractor partitionKeyExtractor )
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
        this.partitionKeyExtractor = partitionKeyExtractor;
    }

}
