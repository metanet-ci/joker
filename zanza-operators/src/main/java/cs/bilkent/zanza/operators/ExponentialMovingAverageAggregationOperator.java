package cs.bilkent.zanza.operators;

import static cs.bilkent.zanza.flow.Port.DEFAULT_PORT_INDEX;
import cs.bilkent.zanza.kvstore.KVStore;
import cs.bilkent.zanza.operator.InitializationContext;
import cs.bilkent.zanza.operator.InvocationContext;
import cs.bilkent.zanza.operator.InvocationResult;
import cs.bilkent.zanza.operator.Operator;
import cs.bilkent.zanza.operator.OperatorConfig;
import cs.bilkent.zanza.operator.PortsToTuples;
import cs.bilkent.zanza.operator.Tuple;
import cs.bilkent.zanza.operator.schema.annotation.OperatorSchema;
import cs.bilkent.zanza.operator.schema.annotation.PortSchema;
import static cs.bilkent.zanza.operator.schema.annotation.PortSchemaScope.EXACT_FIELD_SET;
import cs.bilkent.zanza.operator.schema.annotation.SchemaField;
import cs.bilkent.zanza.operator.spec.OperatorSpec;
import cs.bilkent.zanza.operator.spec.OperatorType;
import cs.bilkent.zanza.scheduling.ScheduleNever;
import static cs.bilkent.zanza.scheduling.ScheduleWhenTuplesAvailable.scheduleWhenTuplesAvailableOnDefaultPort;
import cs.bilkent.zanza.scheduling.SchedulingStrategy;

@OperatorSpec( type = OperatorType.STATEFUL, inputPortCount = 1, outputPortCount = 1 )
@OperatorSchema( inputs = {}, outputs = { @PortSchema( portIndex = DEFAULT_PORT_INDEX, scope = EXACT_FIELD_SET,
        fields = { @SchemaField(
                name = ExponentialMovingAverageAggregationOperator.VALUE_FIELD,
                type = double.class ) } ) } )
public class ExponentialMovingAverageAggregationOperator implements Operator
{

    public static final String WEIGHT_CONFIG_PARAMETER = "weight";

    public static final String FIELD_NAME_CONFIG_PARAMETER = "fieldName";

    public static final String VALUE_FIELD = "value";

    static final String CURRENT_WINDOW_KEY = "currentWindow";

    static final String TUPLE_COUNT_FIELD = "count";

    private double weight;

    private String fieldName;

    @Override
    public SchedulingStrategy init ( final InitializationContext context )
    {
        final OperatorConfig config = context.getConfig();
        this.weight = config.getOrFail( WEIGHT_CONFIG_PARAMETER );
        this.fieldName = config.getOrFail( FIELD_NAME_CONFIG_PARAMETER );

        return scheduleWhenTuplesAvailableOnDefaultPort( 1 );
    }

    @Override
    public InvocationResult process ( final InvocationContext invocationContext )
    {
        final SchedulingStrategy nextStrategy = invocationContext.isSuccessfulInvocation()
                                                ? scheduleWhenTuplesAvailableOnDefaultPort( 1 )
                                                : ScheduleNever.INSTANCE;
        final PortsToTuples output = new PortsToTuples();

        final KVStore kvStore = invocationContext.getKVStore();
        final PortsToTuples input = invocationContext.getInputTuples();

        final Tuple currentWindow = kvStore.getOrDefault( CURRENT_WINDOW_KEY, Tuple::new );

        double value = currentWindow.getDoubleValueOrDefault( VALUE_FIELD, 0d );
        int tupleCount = currentWindow.getIntegerValueOrDefault( TUPLE_COUNT_FIELD, 0 );

        for ( Tuple tuple : input.getTuplesByDefaultPort() )
        {
            final double tupleValue = tuple.getDoubleValueOrDefault( fieldName, 0d );
            value = ( tupleCount++ == 0 ) ? tupleValue : ( weight * tupleValue + ( 1 - weight ) * value );
            final Tuple avgTuple = new Tuple();
            avgTuple.set( VALUE_FIELD, value );
            output.add( avgTuple );
        }

        currentWindow.set( VALUE_FIELD, value );
        currentWindow.set( TUPLE_COUNT_FIELD, tupleCount );

        kvStore.set( CURRENT_WINDOW_KEY, currentWindow );

        return new InvocationResult( nextStrategy, output );
    }

}
