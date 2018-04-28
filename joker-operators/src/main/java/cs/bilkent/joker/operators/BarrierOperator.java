package cs.bilkent.joker.operators;

import java.util.function.BiConsumer;
import java.util.stream.IntStream;

import cs.bilkent.joker.operator.InitCtx;
import cs.bilkent.joker.operator.InvocationCtx;
import cs.bilkent.joker.operator.Operator;
import cs.bilkent.joker.operator.OperatorConfig;
import cs.bilkent.joker.operator.Tuple;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.AT_LEAST_BUT_SAME_ON_ALL_PORTS;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.scheduleWhenTuplesAvailableOnAll;
import cs.bilkent.joker.operator.scheduling.SchedulingStrategy;
import cs.bilkent.joker.operator.schema.runtime.TupleSchema;
import cs.bilkent.joker.operator.spec.OperatorSpec;
import static cs.bilkent.joker.operator.spec.OperatorType.STATEFUL;


/**
 * Produces an single output tuple with the given merge function using a single tuple from each one of the input ports.
 */
@OperatorSpec( type = STATEFUL, outputPortCount = 1 )
public class BarrierOperator implements Operator
{

    public static final String MERGE_POLICY_CONfIG_PARAMETER = "mergePolicy";


    public enum TupleValueMergePolicy
    {
        KEEP_EXISTING_VALUE, OVERWRITE_WITH_NEW_VALUE
    }


    private int inputPortCount;

    private TupleValueMergePolicy mergePolicy;

    private EntryMerger entryMerger;

    private TupleSchema outputSchema;

    @Override
    public SchedulingStrategy init ( final InitCtx ctx )
    {
        final OperatorConfig config = ctx.getConfig();
        this.inputPortCount = ctx.getInputPortCount();

        this.mergePolicy = config.getOrFail( MERGE_POLICY_CONfIG_PARAMETER );
        this.entryMerger = new EntryMerger( mergePolicy );
        this.outputSchema = ctx.getOutputPortSchema( 0 );

        final int[] inputPorts = IntStream.range( 0, inputPortCount ).toArray();
        return scheduleWhenTuplesAvailableOnAll( AT_LEAST_BUT_SAME_ON_ALL_PORTS, ctx.getInputPortCount(), 1, inputPorts );
    }

    @Override
    public void invoke ( final InvocationCtx ctx )
    {
        for ( int i = 0, tupleCount = getTupleCountIfSameOnAllPorts( ctx ); i < tupleCount; i++ )
        {
            final Tuple result = new Tuple( outputSchema );
            entryMerger.target = result;
            for ( int j = 0; j < inputPortCount; j++ )
            {
                final Tuple input = ctx.getInputTupleOrFail( j, i );
                result.attachTo( input );
                input.sinkTo( entryMerger );
            }

            ctx.output( result );
        }
    }

    private int getTupleCountIfSameOnAllPorts ( final InvocationCtx ctx )
    {
        int tupleCount = ctx.getInputTupleCount( 0 );
        if ( ctx.isSuccessfulInvocation() )
        {
            return tupleCount;
        }

        for ( int i = 1; i < inputPortCount; i++ )
        {
            if ( ctx.getInputTupleCount( i ) != tupleCount )
            {
                return getMinTupleCountOfAllPorts( ctx );
            }
        }

        return tupleCount;
    }

    private int getMinTupleCountOfAllPorts ( final InvocationCtx ctx )
    {
        int tupleCount = Integer.MAX_VALUE;
        for ( int i = 0; i < inputPortCount; i++ )
        {
            final int t = ctx.getInputTupleCount( i );
            if ( t < tupleCount )
            {
                tupleCount = t;
            }
        }

        return tupleCount;
    }


    private static class EntryMerger implements BiConsumer<String, Object>
    {

        private final TupleValueMergePolicy mergePolicy;

        EntryMerger ( final TupleValueMergePolicy mergePolicy )
        {
            this.mergePolicy = mergePolicy;
        }

        private Tuple target;

        @Override
        public void accept ( final String sourceKey, final Object sourceValue )
        {
            final Object targetValue = target.get( sourceKey );
            if ( targetValue != null )
            {
                final Object finalValue;
                switch ( mergePolicy )
                {
                    case KEEP_EXISTING_VALUE:
                        finalValue = targetValue;
                        break;
                    case OVERWRITE_WITH_NEW_VALUE:
                        finalValue = sourceValue;
                        break;
                    default:
                        throw new IllegalStateException( "invalid merge policy!" );
                }

                target.set( sourceKey, finalValue );
            }
            else
            {
                target.set( sourceKey, sourceValue );
            }
        }

    }

}
