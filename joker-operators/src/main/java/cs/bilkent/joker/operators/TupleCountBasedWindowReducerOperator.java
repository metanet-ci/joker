package cs.bilkent.joker.operators;

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

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
import static cs.bilkent.joker.operator.schema.annotation.PortSchemaScope.EXACT_FIELD_SET;
import static cs.bilkent.joker.operator.schema.annotation.PortSchemaScope.EXTENDABLE_FIELD_SET;
import cs.bilkent.joker.operator.schema.annotation.SchemaField;
import cs.bilkent.joker.operator.schema.runtime.PortRuntimeSchemaBuilder;
import cs.bilkent.joker.operator.schema.runtime.RuntimeSchemaField;
import cs.bilkent.joker.operator.schema.runtime.TupleSchema;
import cs.bilkent.joker.operator.spec.OperatorSpec;
import static cs.bilkent.joker.operator.spec.OperatorType.PARTITIONED_STATEFUL;
import static java.util.Arrays.asList;


@OperatorSpec( type = PARTITIONED_STATEFUL, inputPortCount = 1, outputPortCount = 1 )
@OperatorSchema( outputs = { @PortSchema( portIndex = DEFAULT_PORT_INDEX, scope = EXTENDABLE_FIELD_SET, fields = { @SchemaField( name = TupleCountBasedWindowReducerOperator.WINDOW_FIELD, type = int.class ) } ) } )
public class TupleCountBasedWindowReducerOperator implements Operator
{

    public static final String TUPLE_COUNT_CONFIG_PARAMETER = "tupleCount";

    public static final String REDUCER_CONFIG_PARAMETER = "reducer";

    public static final String ACCUMULATOR_INITIALIZER_CONFIG_PARAMETER = "accumulatorInitializer";

    public static final String WINDOW_FIELD = "window";

    static final String CURRENT_WINDOW_KEY = "currentWindow";

    static final String ACCUMULATOR_TUPLE_KEY = "accumulator";

    static final String TUPLE_COUNT_FIELD = "count";


    private BiConsumer<Tuple, Tuple> reducer;

    private int tupleCount;

    private TupleSchema outputSchema;

    private Consumer<Tuple> accumulatorInitializer;

    private Supplier<Tuple> accumulatorSupplier;

    private TupleSchema windowSchema;

    @Override
    public SchedulingStrategy init ( final InitializationContext context )
    {
        final OperatorConfig config = context.getConfig();

        this.reducer = config.getOrFail( REDUCER_CONFIG_PARAMETER );
        this.tupleCount = config.getOrFail( TUPLE_COUNT_CONFIG_PARAMETER );
        this.accumulatorInitializer = config.getOrFail( ACCUMULATOR_INITIALIZER_CONFIG_PARAMETER );
        this.accumulatorSupplier = () -> {
            final Tuple accumulator = new Tuple( outputSchema );
            accumulatorInitializer.accept( accumulator );
            return accumulator;
        };
        this.outputSchema = context.getOutputPortSchema( 0 );
        this.windowSchema = new PortRuntimeSchemaBuilder( EXACT_FIELD_SET,
                                                          asList( new RuntimeSchemaField( TUPLE_COUNT_FIELD, Integer.class ),
                                                                  new RuntimeSchemaField( WINDOW_FIELD, Integer.class ) ) ).build();

        return scheduleWhenTuplesAvailableOnDefaultPort( 1 );
    }

    @Override
    public void invoke ( final InvocationContext context )
    {
        final Tuples input = context.getInput();
        final Tuples output = context.getOutput();

        final KVStore kvStore = context.getKVStore();

        final Tuple window = kvStore.getOrDefault( CURRENT_WINDOW_KEY, () -> new Tuple( windowSchema ) );
        int currentTupleCount = window.getIntegerOrDefault( TUPLE_COUNT_FIELD, 0 );
        int windowCount = window.getIntegerOrDefault( WINDOW_FIELD, 0 );
        Tuple accumulator = kvStore.getOrDefault( ACCUMULATOR_TUPLE_KEY, accumulatorSupplier );

        for ( Tuple tuple : input.getTuplesByDefaultPort() )
        {
            reducer.accept( accumulator, tuple );

            if ( ++currentTupleCount == tupleCount )
            {
                currentTupleCount = 0;
                accumulator.set( WINDOW_FIELD, windowCount++ );

                output.add( accumulator );
                accumulator = accumulatorSupplier.get();
            }
        }

        window.set( WINDOW_FIELD, windowCount );
        window.set( TUPLE_COUNT_FIELD, currentTupleCount );

        kvStore.set( CURRENT_WINDOW_KEY, window );
        kvStore.set( ACCUMULATOR_TUPLE_KEY, accumulator );
    }

}
