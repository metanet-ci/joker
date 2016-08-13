package cs.bilkent.joker.operators;

import java.util.function.BiFunction;

import static cs.bilkent.joker.flow.Port.DEFAULT_PORT_INDEX;
import cs.bilkent.joker.operator.InitializationContext;
import cs.bilkent.joker.operator.InvocationContext;
import cs.bilkent.joker.operator.Operator;
import cs.bilkent.joker.operator.OperatorConfig;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.Tuples;
import cs.bilkent.joker.operator.kvstore.KVStore;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.scheduleWhenTuplesAvailableOnDefaultPort;
import cs.bilkent.joker.operator.scheduling.SchedulingStrategy;
import cs.bilkent.joker.operator.schema.annotation.OperatorSchema;
import cs.bilkent.joker.operator.schema.annotation.PortSchema;
import static cs.bilkent.joker.operator.schema.annotation.PortSchemaScope.EXTENDABLE_FIELD_SET;
import cs.bilkent.joker.operator.schema.annotation.SchemaField;
import cs.bilkent.joker.operator.spec.OperatorSpec;
import static cs.bilkent.joker.operator.spec.OperatorType.PARTITIONED_STATEFUL;


@OperatorSpec( type = PARTITIONED_STATEFUL, inputPortCount = 1, outputPortCount = 1 )
@OperatorSchema( inputs = {}, outputs = { @PortSchema( portIndex = DEFAULT_PORT_INDEX, scope = EXTENDABLE_FIELD_SET, fields = {
    @SchemaField( name = TupleCountBasedWindowReducerOperator.WINDOW_FIELD, type = int.class ) } ) } )
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
    }

}
