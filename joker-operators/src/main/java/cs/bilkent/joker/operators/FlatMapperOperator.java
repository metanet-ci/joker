package cs.bilkent.joker.operators;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

import cs.bilkent.joker.operator.InitCtx;
import cs.bilkent.joker.operator.InvocationCtx;
import cs.bilkent.joker.operator.Operator;
import cs.bilkent.joker.operator.OperatorConfig;
import cs.bilkent.joker.operator.Tuple;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.scheduleWhenTuplesAvailableOnDefaultPort;
import cs.bilkent.joker.operator.scheduling.SchedulingStrategy;
import cs.bilkent.joker.operator.schema.runtime.TupleSchema;
import cs.bilkent.joker.operator.spec.OperatorSpec;
import static cs.bilkent.joker.operator.spec.OperatorType.STATELESS;

/**
 * Maps an input tuple to a collection of tuples, and flattens them.
 */
@OperatorSpec( type = STATELESS, inputPortCount = 1, outputPortCount = 1 )
public class FlatMapperOperator implements Operator
{

    public static final String FLAT_MAPPER_CONFIG_PARAMETER = "flatMapper";

    private static final int DEFAULT_TUPLE_COUNT_CONFIG_VALUE = 1;


    private FlatMapperConsumer flatMapper;

    private TupleSchema outputSchema;

    private Supplier<Tuple> outputTupleSupplier;

    @Override
    public SchedulingStrategy init ( final InitCtx ctx )
    {
        final OperatorConfig config = ctx.getConfig();

        this.flatMapper = config.getOrFail( FLAT_MAPPER_CONFIG_PARAMETER );
        this.outputSchema = ctx.getOutputPortSchema( 0 );
        // TODO provide output schema to the Tuple c'tor
        this.outputTupleSupplier = Tuple::new;

        return scheduleWhenTuplesAvailableOnDefaultPort( DEFAULT_TUPLE_COUNT_CONFIG_VALUE );
    }

    @Override
    public void invoke ( final InvocationCtx ctx )
    {
        final List<Tuple> tuples = ctx.getInputTuplesByDefaultPort();
        for ( int i = 0, j = tuples.size(); i < j; i++ )
        {
            final Tuple input = tuples.get( i );
            flatMapper.accept( input, outputTupleSupplier, output -> {
                output.attachTo( input );
                ctx.output( output );
            } );
        }
    }

    @FunctionalInterface
    public interface FlatMapperConsumer
    {
        void accept ( Tuple input, Supplier<Tuple> outputTupleSupplier, Consumer<Tuple> outputCollector );
    }

}
