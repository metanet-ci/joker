package cs.bilkent.joker.operators;

import static cs.bilkent.joker.flow.Port.DEFAULT_PORT_INDEX;
import cs.bilkent.joker.operator.InitializationContext;
import cs.bilkent.joker.operator.InvocationContext;
import cs.bilkent.joker.operator.Operator;
import cs.bilkent.joker.operator.OperatorConfig;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.kvstore.KVStore;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.scheduleWhenTuplesAvailableOnDefaultPort;
import cs.bilkent.joker.operator.scheduling.SchedulingStrategy;
import cs.bilkent.joker.operator.schema.annotation.OperatorSchema;
import cs.bilkent.joker.operator.schema.annotation.PortSchema;
import static cs.bilkent.joker.operator.schema.annotation.PortSchemaScope.EXACT_FIELD_SET;
import cs.bilkent.joker.operator.schema.annotation.SchemaField;
import cs.bilkent.joker.operator.schema.runtime.PortRuntimeSchemaBuilder;
import cs.bilkent.joker.operator.schema.runtime.RuntimeSchemaField;
import cs.bilkent.joker.operator.schema.runtime.TupleSchema;
import cs.bilkent.joker.operator.spec.OperatorSpec;
import static cs.bilkent.joker.operator.spec.OperatorType.STATEFUL;
import static java.util.Arrays.asList;


/**
 * Produces output tuples that contain Exponential Moving Average of values of input tuples.
 *
 * @see <a href="https://en.wikipedia.org/wiki/Moving_average#Exponential_moving_average">Exponential Moving Average Wikipedia</a>
 */

@OperatorSpec( type = STATEFUL, inputPortCount = 1, outputPortCount = 1 )
@OperatorSchema( outputs = { @PortSchema( portIndex = DEFAULT_PORT_INDEX, scope = EXACT_FIELD_SET, fields = { @SchemaField( name = ExponentialMovingAverageAggregationOperator.VALUE_FIELD, type = double.class ) } ) } )
public class ExponentialMovingAverageAggregationOperator implements Operator
{

    public static final String WEIGHT_CONFIG_PARAMETER = "weight";

    public static final String FIELD_NAME_CONFIG_PARAMETER = "fieldName";

    public static final String VALUE_FIELD = "value";

    static final String CURRENT_WINDOW_KEY = "currentWindow";

    static final String TUPLE_COUNT_FIELD = "count";


    private TupleSchema outputSchema;

    private double weight;

    private String fieldName;

    private TupleSchema windowSchema;

    @Override
    public SchedulingStrategy init ( final InitializationContext ctx )
    {
        this.outputSchema = ctx.getOutputPortSchema( 0 );

        final OperatorConfig config = ctx.getConfig();
        this.weight = config.getOrFail( WEIGHT_CONFIG_PARAMETER );
        this.fieldName = config.getOrFail( FIELD_NAME_CONFIG_PARAMETER );
        this.windowSchema = new PortRuntimeSchemaBuilder( EXACT_FIELD_SET,
                                                          asList( new RuntimeSchemaField( VALUE_FIELD, Double.class ),
                                                                  new RuntimeSchemaField( TUPLE_COUNT_FIELD, Integer.class ) ) ).build();

        return scheduleWhenTuplesAvailableOnDefaultPort( 1 );
    }

    @Override
    public void invoke ( final InvocationContext ctx )
    {
        final KVStore kvStore = ctx.getKVStore();

        final Tuple currentWindow = kvStore.getOrDefault( CURRENT_WINDOW_KEY, () -> new Tuple( windowSchema ) );

        double value = currentWindow.getDoubleValueOrDefault( VALUE_FIELD, 0d );
        int tupleCount = currentWindow.getIntegerValueOrDefault( TUPLE_COUNT_FIELD, 0 );

        for ( Tuple tuple : ctx.getInputTuplesByDefaultPort() )
        {
            final double tupleValue = tuple.getDoubleValueOrDefault( fieldName, 0d );
            value = ( tupleCount++ == 0 ) ? tupleValue : ( weight * tupleValue + ( 1 - weight ) * value );
            final Tuple avgTuple = new Tuple( outputSchema );
            avgTuple.set( VALUE_FIELD, value );

            ctx.output( avgTuple );
        }

        currentWindow.set( VALUE_FIELD, value );
        currentWindow.set( TUPLE_COUNT_FIELD, tupleCount );

        kvStore.set( CURRENT_WINDOW_KEY, currentWindow );
    }

}
