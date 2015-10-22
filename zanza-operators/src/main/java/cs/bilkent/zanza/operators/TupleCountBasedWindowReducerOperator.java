package cs.bilkent.zanza.operators;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import cs.bilkent.zanza.operator.InvocationReason;
import cs.bilkent.zanza.operator.Operator;
import cs.bilkent.zanza.operator.OperatorConfig;
import cs.bilkent.zanza.operator.OperatorContext;
import cs.bilkent.zanza.operator.OperatorSpec;
import cs.bilkent.zanza.operator.OperatorType;
import cs.bilkent.zanza.operator.PortsToTuples;
import cs.bilkent.zanza.operator.ProcessingResult;
import cs.bilkent.zanza.operator.Tuple;
import cs.bilkent.zanza.operator.TupleAccessor;
import cs.bilkent.zanza.operator.kvstore.KVStore;
import cs.bilkent.zanza.operator.scheduling.ScheduleNever;
import static cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.scheduleWhenTuplesAvailableOnDefaultPort;
import cs.bilkent.zanza.operator.scheduling.SchedulingStrategy;

@OperatorSpec( type = OperatorType.PARTITIONED_STATEFUL, inputPortCount = 1, outputPortCount = 1 )
public class TupleCountBasedWindowReducerOperator implements Operator
{

    public static final String TUPLE_COUNT_CONFIG_PARAMETER = "tupleCount";

    public static final String REDUCER_CONFIG_PARAMETER = "reducer";

    public static final String INITIAL_VALUE_CONFIG_PARAMETER = "initialValue";

    private static final String CURRENT_WINDOW_KEY = "currentWindow";

    private static final String ACCUMULATOR_TUPLE_KEY = "currentTuple";

    static final String WINDOW_FIELD = "window";

    static final String TUPLE_COUNT_FIELD = "count";


    private KVStore kvStore;

    private BiFunction<Tuple, Tuple, Tuple> reducerFunc;

    private int tupleCount;

    private Tuple initialValue;

    @Override
    public SchedulingStrategy init ( final OperatorContext context )
    {
        final OperatorConfig config = context.getConfig();

        Object reducerFuncObject = config.getObject( REDUCER_CONFIG_PARAMETER );
        if ( reducerFuncObject instanceof BiFunction )
        {
            this.reducerFunc = (BiFunction<Tuple, Tuple, Tuple>) reducerFuncObject;
        }
        else
        {
            throw new IllegalArgumentException( "reducer function is not provided!" );
        }

        if ( config.contains( TUPLE_COUNT_CONFIG_PARAMETER ) )
        {
            this.tupleCount = config.getInteger( TUPLE_COUNT_CONFIG_PARAMETER );
        }
        else
        {
            throw new IllegalArgumentException( "tuple count is not specified!" );
        }

        this.kvStore = context.getKVStore();

        Object initialValueObject = config.getObject( INITIAL_VALUE_CONFIG_PARAMETER );

        if ( initialValueObject instanceof Tuple )
        {
            this.initialValue = (Tuple) initialValueObject;
        }

        return scheduleWhenTuplesAvailableOnDefaultPort( 1 );
    }

    @Override
    public ProcessingResult process ( final PortsToTuples portsToTuples, final InvocationReason reason )
    {
        final PortsToTuples result = new PortsToTuples();
        final SchedulingStrategy nextStrategy;

        if ( reason.isSuccessful() )
        {
            final Map<Object, Tuple> windows = new HashMap<>();
            final Map<Object, Tuple> accumulators = new HashMap<>();

            compute( result, portsToTuples.getTuplesByDefaultPort(), windows, accumulators );
            persist( windows, accumulators );

            nextStrategy = scheduleWhenTuplesAvailableOnDefaultPort( 1 );
        }
        else
        {
            nextStrategy = ScheduleNever.INSTANCE;
        }

        return new ProcessingResult( nextStrategy, result );
    }

    private void compute ( final PortsToTuples result,
                           final List<Tuple> tuples,
                           final Map<Object, Tuple> windows,
                           final Map<Object, Tuple> accumulators )
    {
        for ( Tuple tuple : tuples )
        {
            final Object key = tuple.getPartitionKey();

            final Tuple window = windows.computeIfAbsent( key, this::getWindow );
            int currentTupleCount = window.getInteger( TUPLE_COUNT_FIELD );
            int windowCount = window.getInteger( WINDOW_FIELD );

            Tuple accumulator = getAccumulator( accumulators, key );

            accumulator = accumulator == null ? tuple : reducerFunc.apply( accumulator, tuple );

            if ( ++currentTupleCount == tupleCount )
            {
                currentTupleCount = 0;
                accumulator.set( "window", windowCount++ );
                TupleAccessor.setPartition( accumulator, key, tuple.getPartitionHash() );

                result.add( accumulator );
                accumulator = null;
            }

            accumulators.put( key, accumulator );

            window.set( WINDOW_FIELD, windowCount );
            window.set( TUPLE_COUNT_FIELD, currentTupleCount );
        }
    }

    private Tuple getAccumulator ( final Map<Object, Tuple> accumulators, final Object key )
    {
        Tuple accumulator = accumulators.containsKey( key ) ? accumulators.remove( key ) : this.kvStore.get( toAccumulatorKey( key ) );

        if ( accumulator == null && initialValue != null )
        {
            accumulator = new Tuple( initialValue.asMap() );
        }

        return accumulator;
    }

    private Tuple getWindow ( final Object key )
    {
        final Tuple window = this.kvStore.get( toWindowKey( key ) );
        return window != null ? window : newEmptyWindow();
    }

    private Tuple newEmptyWindow ()
    {
        final Tuple tuple = new Tuple();
        tuple.set( WINDOW_FIELD, 0 );
        tuple.set( TUPLE_COUNT_FIELD, 0 );
        return tuple;
    }

    private void persist ( final Map<Object, Tuple> windows, final Map<Object, Tuple> accumulators )
    {
        for ( Map.Entry<Object, Tuple> entry : windows.entrySet() )
        {
            this.kvStore.set( toWindowKey( entry.getKey() ), entry.getValue() );
        }

        for ( Map.Entry<Object, Tuple> entry : accumulators.entrySet() )
        {
            final String accumulatorKey = toAccumulatorKey( entry.getKey() );
            final Tuple accumulator = entry.getValue();

            if ( accumulator != null )
            {
                this.kvStore.set( accumulatorKey, entry.getValue() );
            }
            else
            {
                this.kvStore.remove( accumulatorKey );
            }
        }
    }

    static String toWindowKey ( final Object key )
    {
        return key + CURRENT_WINDOW_KEY;
    }

    static String toAccumulatorKey ( final Object key )
    {
        return key + ACCUMULATOR_TUPLE_KEY;
    }

}
