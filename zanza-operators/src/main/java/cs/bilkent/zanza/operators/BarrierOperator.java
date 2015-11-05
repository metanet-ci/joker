package cs.bilkent.zanza.operators;

import java.util.Map;
import java.util.Optional;
import java.util.function.BinaryOperator;
import java.util.stream.IntStream;

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
import static cs.bilkent.zanza.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.AT_LEAST_BUT_SAME_ON_ALL_PORTS;
import static cs.bilkent.zanza.scheduling.ScheduleWhenTuplesAvailable.scheduleWhenTuplesAvailableOnAll;
import cs.bilkent.zanza.scheduling.SchedulingStrategy;

@OperatorSpec( type = OperatorType.STATELESS, outputPortCount = 1 )
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

    private int[] inputPorts;


    @Override
    public SchedulingStrategy init ( final InitializationContext context )
    {
        final OperatorConfig config = context.getConfig();

        final TupleValueMergePolicy mergePolicy = config.getOrFail( MERGE_POLICY_CONfIG_PARAMETER );
        this.tupleMergeFunc = new TupleMerger( mergePolicy );
        this.inputPorts = IntStream.range( 0, config.getInputPortCount() ).toArray();

        return getSchedulingStrategyForInputPorts();
    }

    private SchedulingStrategy getSchedulingStrategyForInputPorts ()
    {
        return scheduleWhenTuplesAvailableOnAll( AT_LEAST_BUT_SAME_ON_ALL_PORTS, 1, inputPorts );
    }

    @Override
    public InvocationResult process ( final InvocationContext invocationContext )
    {
        final PortsToTuples output;
        final SchedulingStrategy next = invocationContext.isSuccessfulInvocation()
                                        ? getSchedulingStrategyForInputPorts()
                                        : ScheduleNever.INSTANCE;

        final PortsToTuples portsToTuples = invocationContext.getInputTuples();
        final Optional<Integer> tupleCountOpt = IntStream.of( inputPorts )
                                                         .mapToObj( portIndex -> portsToTuples.getTuples( portIndex ).size() )
                                                         .reduce( ( count1, count2 ) -> count1.equals( count2 ) ? count1 : 0 );
        final int tupleCount = tupleCountOpt.orElse( 0 );
        if ( tupleCount == 0 )
        {
            throw new IllegalArgumentException( "number of tuples are not equal for all ports!" );
        }

        output = IntStream.range( 0, tupleCount )
                          .mapToObj( tupleIndex -> IntStream.of( inputPorts )
                                                            .mapToObj( portIndex -> portsToTuples.getTuple( portIndex, tupleIndex ) ) )
                          .map( tuples -> tuples.reduce( new Tuple(), tupleMergeFunc ) )
                          .collect( PortsToTuples.COLLECT_TO_DEFAULT_PORT );

        final Tuple result = IntStream.of( inputPorts )
                                      .mapToObj( portIndex -> portsToTuples.getTuple( portIndex, 0 ) )
                                      .reduce( new Tuple(), tupleMergeFunc );

        output.add( result );

        return new InvocationResult( next, output );
    }
}
