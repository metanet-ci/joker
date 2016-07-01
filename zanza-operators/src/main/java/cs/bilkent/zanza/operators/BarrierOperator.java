package cs.bilkent.zanza.operators;

import java.util.Map;
import java.util.function.BinaryOperator;
import java.util.stream.IntStream;

import cs.bilkent.zanza.operator.InitializationContext;
import cs.bilkent.zanza.operator.InvocationContext;
import cs.bilkent.zanza.operator.Operator;
import cs.bilkent.zanza.operator.OperatorConfig;
import cs.bilkent.zanza.operator.Tuple;
import cs.bilkent.zanza.operator.Tuples;
import static cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.AT_LEAST_BUT_SAME_ON_ALL_PORTS;
import static cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.scheduleWhenTuplesAvailableOnAll;
import cs.bilkent.zanza.operator.scheduling.SchedulingStrategy;
import cs.bilkent.zanza.operator.spec.OperatorSpec;
import static cs.bilkent.zanza.operator.spec.OperatorType.STATEFUL;


/**
 * Produces an single output tuple with the given merge function using a single tuple from each one of the input ports.
 */
@OperatorSpec( type = STATEFUL, outputPortCount = 1 )
public class BarrierOperator implements Operator
{

    public static final String MERGE_POLICY_CONfIG_PARAMETER = "mergePolicy";


    public enum TupleValueMergePolicy
    {
        KEEP_EXISTING_VALUE,
        OVERWRITE_WITH_NEW_VALUE
    }


    private static class TupleMerger implements BinaryOperator<Tuple>
    {

        private final TupleValueMergePolicy mergePolicy;

        TupleMerger ( final TupleValueMergePolicy mergePolicy )
        {
            this.mergePolicy = mergePolicy;
        }

        // modifies first parameter
        @Override
        public Tuple apply ( final Tuple tuple1, final Tuple tuple2 )
        {
            for ( Map.Entry<String, Object> tuple2KeyValue : tuple2.asMap().entrySet() )
            {
                final String tuple2Key = tuple2KeyValue.getKey();
                final Object tuple1Value = tuple1.get( tuple2Key );
                if ( tuple1Value != null )
                {
                    final Object finalValue;
                    switch ( mergePolicy )
                    {
                        case KEEP_EXISTING_VALUE:
                            finalValue = tuple1Value;
                            break;
                        case OVERWRITE_WITH_NEW_VALUE:
                            finalValue = tuple2KeyValue.getValue();
                            break;
                        default:
                            throw new IllegalStateException( "invalid merge policy!" );
                    }

                    tuple1.put( tuple2Key, finalValue );
                }
                else
                {
                    tuple1.put( tuple2Key, tuple2KeyValue.getValue() );
                }
            }

            return tuple1;
        }
    }


    private int inputPortCount;

    private BinaryOperator<Tuple> tupleMergeFunc;

    @Override
    public SchedulingStrategy init ( final InitializationContext context )
    {
        final OperatorConfig config = context.getConfig();
        this.inputPortCount = context.getInputPortCount();

        final TupleValueMergePolicy mergePolicy = config.getOrFail( MERGE_POLICY_CONfIG_PARAMETER );
        this.tupleMergeFunc = new TupleMerger( mergePolicy );

        final int[] inputPorts = IntStream.range( 0, inputPortCount ).toArray();
        return scheduleWhenTuplesAvailableOnAll( AT_LEAST_BUT_SAME_ON_ALL_PORTS, context.getInputPortCount(), 1, inputPorts );
    }

    @Override
    public void invoke ( final InvocationContext invocationContext )
    {
        final Tuples input = invocationContext.getInput();
        final Tuples output = invocationContext.getOutput();

        int tupleCount = getTupleCountIfSameOnAllPorts( input );
        if ( tupleCount == 0 )
        {
            if ( invocationContext.isSuccessfulInvocation() )
            {
                throw new IllegalArgumentException( "number of tuples are not equal for all ports!" );
            }
            else
            {
                tupleCount = getMinTupleCountOfAllPorts( input );
            }
        }

        for ( int i = 0; i < tupleCount; i++ )
        {
            Tuple prev = new Tuple();
            for ( int j = 0; j < inputPortCount; j++ )
            {
                final Tuple tuple = input.getTupleOrFail( j, i );
                prev = tupleMergeFunc.apply( prev, tuple );
            }

            output.add( prev );
        }
    }

    private int getTupleCountIfSameOnAllPorts ( final Tuples input )
    {
        int tupleCount = input.getTupleCount( 0 );
        for ( int i = 1; i < inputPortCount; i++ )
        {
            if ( input.getTupleCount( i ) != tupleCount )
            {
                return 0;
            }
        }

        return tupleCount;
    }

    private int getMinTupleCountOfAllPorts ( final Tuples input )
    {
        int tupleCount = Integer.MAX_VALUE;
        for ( int i = 0; i < inputPortCount; i++ )
        {
            final int t = input.getTupleCount( i );
            if ( t < tupleCount )
            {
                tupleCount = t;
            }
        }

        return tupleCount;
    }

}
