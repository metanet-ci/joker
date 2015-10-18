package cs.bilkent.zanza.operators;

import java.util.List;
import java.util.Map;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;

import cs.bilkent.zanza.operator.InvocationReason;
import cs.bilkent.zanza.operator.Operator;
import cs.bilkent.zanza.operator.OperatorContext;
import cs.bilkent.zanza.operator.OperatorSpec;
import cs.bilkent.zanza.operator.OperatorType;
import cs.bilkent.zanza.operator.PortsToTuples;
import cs.bilkent.zanza.operator.ProcessingResult;
import cs.bilkent.zanza.operator.Tuple;
import cs.bilkent.zanza.operator.invocationreason.SuccessfulInvocation;
import cs.bilkent.zanza.operator.scheduling.ScheduleNever;
import static cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.scheduleWhenTuplesAvailableOnAll;
import cs.bilkent.zanza.operator.scheduling.SchedulingStrategy;

@OperatorSpec( type = OperatorType.PARTITIONED_STATEFUL, outputPortCount = 1 )
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

        public TupleMerger ( final TupleValueMergePolicy mergePolicy )
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


    private BinaryOperator<Tuple> tupleMergeFunc;

    private List<Integer> inputPortIndices;


    @Override
    public SchedulingStrategy init ( final OperatorContext context )
    {
        final Object mergePolicyObject = context.getConfig().getObject( MERGE_POLICY_CONfIG_PARAMETER );
        if ( mergePolicyObject instanceof TupleValueMergePolicy )
        {
            this.tupleMergeFunc = new TupleMerger( (TupleValueMergePolicy) mergePolicyObject );
        }
        else
        {
            throw new IllegalArgumentException( "merge policy is missing!" );
        }

        this.inputPortIndices = context.getInputPorts().stream().map( port -> port.portIndex ).collect( Collectors.toList() );
        return getSchedulingStrategyForInputPorts();
    }

    private SchedulingStrategy getSchedulingStrategyForInputPorts ()
    {

        return scheduleWhenTuplesAvailableOnAll( 1, inputPortIndices );
    }

    @Override
    public ProcessingResult process ( final PortsToTuples portsToTuples, final InvocationReason reason )
    {
        final PortsToTuples output = new PortsToTuples();
        final SchedulingStrategy next;
        if ( reason == SuccessfulInvocation.INSTANCE )
        {
            final Tuple result = inputPortIndices.stream()
                                                 .map( portIndex -> portsToTuples.getTuple( portIndex, 0 ) )
                                                 .reduce( new Tuple(), tupleMergeFunc );

            output.add( result );
            next = getSchedulingStrategyForInputPorts();
        }
        else
        {
            next = ScheduleNever.INSTANCE;
        }

        return new ProcessingResult( next, output );
    }
}
