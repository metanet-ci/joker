package cs.bilkent.zanza.operators;

import java.util.function.BiFunction;

import cs.bilkent.zanza.kvstore.KVStore;
import cs.bilkent.zanza.operator.InitializationContext;
import cs.bilkent.zanza.operator.InvocationContext;
import cs.bilkent.zanza.operator.InvocationResult;
import cs.bilkent.zanza.operator.Operator;
import cs.bilkent.zanza.operator.OperatorConfig;
import cs.bilkent.zanza.operator.OperatorSpec;
import cs.bilkent.zanza.operator.OperatorType;
import cs.bilkent.zanza.operator.PortsToTuples;
import cs.bilkent.zanza.operator.Tuple;
import cs.bilkent.zanza.scheduling.ScheduleNever;
import static cs.bilkent.zanza.scheduling.ScheduleWhenTuplesAvailable.scheduleWhenTuplesAvailableOnDefaultPort;
import cs.bilkent.zanza.scheduling.SchedulingStrategy;

@OperatorSpec( type = OperatorType.PARTITIONED_STATEFUL, inputPortCount = 1, outputPortCount = 1 )
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

    // TODO finalizer function that can be applied after all reduces are done for a window ???

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
    public InvocationResult process ( final InvocationContext invocationContext )
    {
        final PortsToTuples result = new PortsToTuples();
        final SchedulingStrategy nextStrategy = invocationContext.isSuccessfulInvocation()
                                                ? scheduleWhenTuplesAvailableOnDefaultPort( 1 )
                                                : ScheduleNever.INSTANCE;

        final KVStore kvStore = invocationContext.getKVStore();

        final Tuple window = kvStore.getOrDefault( CURRENT_WINDOW_KEY, Tuple::new );
        int currentTupleCount = window.getIntegerOrDefault( TUPLE_COUNT_FIELD, 0 );
        int windowCount = window.getIntegerOrDefault( WINDOW_FIELD, 0 );
        Tuple accumulator = kvStore.getOrDefault( ACCUMULATOR_TUPLE_KEY, initialValue );

        for ( Tuple tuple : invocationContext.getInputTuples().getTuplesByDefaultPort() )
        {
            accumulator = accumulator == null ? tuple : reducerFunc.apply( accumulator, tuple );

            if ( ++currentTupleCount == tupleCount )
            {
                currentTupleCount = 0;
                accumulator.set( WINDOW_FIELD, windowCount++ );

                result.add( accumulator );
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

        return new InvocationResult( nextStrategy, result );
    }

}
