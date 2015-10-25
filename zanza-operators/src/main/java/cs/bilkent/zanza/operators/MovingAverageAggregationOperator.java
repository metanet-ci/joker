package cs.bilkent.zanza.operators;

import cs.bilkent.zanza.operator.InitializationContext;
import cs.bilkent.zanza.operator.InvocationContext;
import cs.bilkent.zanza.operator.InvocationResult;
import cs.bilkent.zanza.operator.KVStore;
import cs.bilkent.zanza.operator.Operator;
import cs.bilkent.zanza.operator.OperatorSpec;
import cs.bilkent.zanza.operator.OperatorType;
import cs.bilkent.zanza.operator.PortsToTuples;
import cs.bilkent.zanza.operator.SchedulingStrategy;
import cs.bilkent.zanza.operator.Tuple;
import cs.bilkent.zanza.operator.scheduling.ScheduleNever;
import static cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.scheduleWhenTuplesAvailableOnDefaultPort;

@OperatorSpec( type = OperatorType.STATEFUL, inputPortCount = 1, outputPortCount = 1 )
public class MovingAverageAggregationOperator implements Operator
{

    public static final String TUPLE_COUNT_CONFIG_PARAMETER = "tupleCount";

    public static final String FIELD_NAME_CONFIG_PARAMETER = "fieldName";

    public static final String CURRENT_AVERAGE_KEY = "currentAvg";

    public static final String WINDOW_COUNT_FIELD = "window";

    public static final String VALUE_FIELD = "value";

    public static final String TUPLE_COUNT_FIELD = "count";

    private int tupleCount;

    private String fieldName;

    @Override
    public SchedulingStrategy init ( final InitializationContext context )
    {
        this.tupleCount = context.getConfig().getOrFail( TUPLE_COUNT_CONFIG_PARAMETER );
        this.fieldName = context.getConfig().getOrFail( FIELD_NAME_CONFIG_PARAMETER );

        return scheduleWhenTuplesAvailableOnDefaultPort( 1 );
    }

    @Override
    public InvocationResult process ( final InvocationContext invocationContext )
    {
        final SchedulingStrategy nextStrategy;
        final PortsToTuples output = new PortsToTuples();

        if ( invocationContext.isSuccessfulInvocation() )
        {
            final KVStore kvStore = invocationContext.getKVStore();
            final PortsToTuples input = invocationContext.getInputTuples();

            final Tuple currentWindow = kvStore.getOrDefault( CURRENT_AVERAGE_KEY, Tuple::new );

            int windowCount = currentWindow.getIntegerValueOrDefault( WINDOW_COUNT_FIELD, 0 );
            double value = currentWindow.getDoubleValueOrDefault( VALUE_FIELD, 0d );
            int tupleCount = currentWindow.getIntegerValueOrDefault( TUPLE_COUNT_FIELD, 0 );

            for ( Tuple tuple : input.getTuplesByDefaultPort() )
            {
                final double tupleValue = tuple.getDoubleValueOrDefault( fieldName, 0d );

                if ( tupleCount < this.tupleCount - 1 )
                {
                    value += tupleValue;
                    tupleCount++;
                }
                else if ( tupleCount == this.tupleCount - 1 )
                {
                    value = ( ( value + tupleValue ) / ++tupleCount );

                    final Tuple avgTuple = createOutputTuple( windowCount, value );
                    output.add( avgTuple );

                    windowCount = 1;
                }
                else
                {
                    value = ( value + ( tupleValue / tupleCount ) - ( ( tupleValue - tupleCount ) / tupleCount ) );

                    final Tuple avgTuple = createOutputTuple( windowCount, value );
                    output.add( avgTuple );

                    windowCount++;
                }
            }

            currentWindow.set( WINDOW_COUNT_FIELD, windowCount );
            currentWindow.set( VALUE_FIELD, value );
            currentWindow.set( TUPLE_COUNT_FIELD, tupleCount );

            kvStore.set( CURRENT_AVERAGE_KEY, currentWindow );

            nextStrategy = scheduleWhenTuplesAvailableOnDefaultPort( 1 );
        }
        else
        {
            nextStrategy = ScheduleNever.INSTANCE;
        }

        return new InvocationResult( nextStrategy, output );
    }

    private Tuple createOutputTuple ( final int windowCount, final double value )
    {
        final Tuple avgTuple = new Tuple();
        avgTuple.set( VALUE_FIELD, value );
        avgTuple.set( WINDOW_COUNT_FIELD, windowCount );
        return avgTuple;
    }
}
