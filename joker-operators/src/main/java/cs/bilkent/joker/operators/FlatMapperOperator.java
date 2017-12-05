package cs.bilkent.joker.operators;

import java.util.function.Consumer;
import java.util.function.Supplier;

import cs.bilkent.joker.operator.InitializationContext;
import cs.bilkent.joker.operator.InvocationContext;
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
    public SchedulingStrategy init ( final InitializationContext ctx )
    {
        final OperatorConfig config = ctx.getConfig();

        this.flatMapper = config.getOrFail( FLAT_MAPPER_CONFIG_PARAMETER );
        this.outputSchema = ctx.getOutputPortSchema( 0 );
        this.outputTupleSupplier = () -> new Tuple( outputSchema );

        return scheduleWhenTuplesAvailableOnDefaultPort( DEFAULT_TUPLE_COUNT_CONFIG_VALUE );
    }

    @Override
    public void invoke ( final InvocationContext ctx )
    {
        final Consumer<Tuple> outputCollector = ctx::output;

        for ( Tuple tuple : ctx.getInputTuplesByDefaultPort() )
        {

            flatMapper.accept( tuple, outputTupleSupplier, outputCollector );
        }
    }

    public interface FlatMapperConsumer
    {
        void accept ( Tuple input, Supplier<Tuple> outputTupleSupplier, Consumer<Tuple> outputCollector );
    }

}
