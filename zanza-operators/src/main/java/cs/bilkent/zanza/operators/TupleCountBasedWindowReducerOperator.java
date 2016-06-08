package cs.bilkent.zanza.operators;

import java.util.function.BiFunction;

import static cs.bilkent.zanza.flow.Port.DEFAULT_PORT_INDEX;
import cs.bilkent.zanza.operator.InitializationContext;
import cs.bilkent.zanza.operator.InvocationContext;
import cs.bilkent.zanza.operator.Operator;
import cs.bilkent.zanza.operator.OperatorConfig;
import cs.bilkent.zanza.operator.Tuple;
import cs.bilkent.zanza.operator.Tuples;
import cs.bilkent.zanza.operator.kvstore.KVStore;
import cs.bilkent.zanza.operator.scheduling.ScheduleNever;
import static cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.scheduleWhenTuplesAvailableOnDefaultPort;
import cs.bilkent.zanza.operator.scheduling.SchedulingStrategy;
import cs.bilkent.zanza.operator.schema.annotation.OperatorSchema;
import cs.bilkent.zanza.operator.schema.annotation.PortSchema;
import static cs.bilkent.zanza.operator.schema.annotation.PortSchemaScope.BASE_FIELD_SET;
import cs.bilkent.zanza.operator.schema.annotation.SchemaField;
import cs.bilkent.zanza.operator.spec.OperatorSpec;
import static cs.bilkent.zanza.operator.spec.OperatorType.PARTITIONED_STATEFUL;


@OperatorSpec( type = PARTITIONED_STATEFUL, inputPortCount = 1, outputPortCount = 1 )
@OperatorSchema( inputs = {}, outputs = { @PortSchema( portIndex = DEFAULT_PORT_INDEX, scope = BASE_FIELD_SET,
        fields = { @SchemaField(
                name = TupleCountBasedWindowReducerOperator.WINDOW_FIELD,
                type = int.class ) } ) } )
public class TupleCountBasedWindowReducerOperator implements Operator
{

    public static final String TUPLE_COUNT_CONFIG_PARAMETER = "tupleCount";

    public static final String REDUCER_CONFIG_PARAMETER = "reducer";

    public static final String INITIAL_VALUE_CONFIG_PARAMETER = "initialValue";

    public static final String WINDOW_FIELD = "window";

    static final String CURRENT_WINDOW_KEY = "currentWindow";

    static final String ACCUMULATOR_TUPLE_KEY = "accumulator";

    static final String TUPLE_COUNT_FIELD = "count";


    private BiFunction<Tuple, Tuple, Tuple> reducerFunc;

    private int tupleCount;

    private Tuple initialValue;

    @Override
    public SchedulingStrategy init ( final InitializationContext context )
    {
        final OperatorConfig config = context.getConfig();

        this.reducerFunc = config.getOrFail( REDUCER_CONFIG_PARAMETER );
        this.tupleCount = config.getOrFail( TUPLE_COUNT_CONFIG_PARAMETER );
        this.initialValue = config.get( INITIAL_VALUE_CONFIG_PARAMETER );

        return scheduleWhenTuplesAvailableOnDefaultPort( 1 );
    }

    @Override
    public void invoke ( final InvocationContext invocationContext )
    {
        final Tuples input = invocationContext.getInput();
        final Tuples output = invocationContext.getOutput();

        final KVStore kvStore = invocationContext.getKVStore();

        final Tuple window = kvStore.getOrDefault( CURRENT_WINDOW_KEY, Tuple::new );
        int currentTupleCount = window.getIntegerOrDefault( TUPLE_COUNT_FIELD, 0 );
        int windowCount = window.getIntegerOrDefault( WINDOW_FIELD, 0 );
        Tuple accumulator = kvStore.getOrDefault( ACCUMULATOR_TUPLE_KEY, initialValue );

        for ( Tuple tuple : input.getTuplesByDefaultPort() )
        {
            accumulator = accumulator == null ? tuple : reducerFunc.apply( accumulator, tuple );

            if ( ++currentTupleCount == tupleCount )
            {
                currentTupleCount = 0;
                accumulator.set( WINDOW_FIELD, windowCount++ );

                output.add( accumulator );
                accumulator = initialValue;
            }
        }

        window.set( WINDOW_FIELD, windowCount );
        window.set( TUPLE_COUNT_FIELD, currentTupleCount );

        kvStore.set( CURRENT_WINDOW_KEY, window );
        if ( accumulator != null )
        {
            kvStore.set( ACCUMULATOR_TUPLE_KEY, accumulator );
        }
        else
        {
            kvStore.remove( ACCUMULATOR_TUPLE_KEY );
        }

        if ( invocationContext.isErroneousInvocation() )
        {
            invocationContext.setNewSchedulingStrategy( ScheduleNever.INSTANCE );
        }
    }

}
