package cs.bilkent.joker.experiment.bargain;

import java.util.List;

import cs.bilkent.joker.operator.InitializationContext;
import cs.bilkent.joker.operator.InvocationContext;
import cs.bilkent.joker.operator.Operator;
import cs.bilkent.joker.operator.OperatorConfig;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.Tuples;
import cs.bilkent.joker.operator.scheduling.ScheduleWhenAvailable;
import cs.bilkent.joker.operator.scheduling.SchedulingStrategy;
import cs.bilkent.joker.operator.spec.OperatorSpec;
import static cs.bilkent.joker.operator.spec.OperatorType.STATEFUL;
import cs.bilkent.joker.utils.Pair;

@OperatorSpec( type = STATEFUL, inputPortCount = 0, outputPortCount = 1 )
public abstract class TickerPriceBaseOperator implements Operator
{

    public static final String MIN_PRICE_CONFIG_PARAMETER = "minPrice";

    public static final String MAX_PRICE_CONFIG_PARAMETER = "maxPrice";

    public static final String TICKERS_PER_TIME_UNIT_CONFIG_PARAMETER = "tickersPerTimeUnit";

    public static final String TUPLES_PER_INVOCATION_CONFIG_PARAMETER = "tuplesPerInvocation";


    private int tickersPerTimeUnit;

    private TickerPriceGenerator tickerPriceGenerator;

    private Thread tickerPriceGeneratorThread;

    private List<Pair<String, Double>> tickerPrices;

    private int i;

    private int j;

    private long timestamp;

    private int tuplesPerInvocation;

    @Override
    public SchedulingStrategy init ( final InitializationContext context )
    {
        final OperatorConfig config = context.getConfig();
        this.tickersPerTimeUnit = config.getInteger( TICKERS_PER_TIME_UNIT_CONFIG_PARAMETER );
        final int minPrice = config.getInteger( MIN_PRICE_CONFIG_PARAMETER );
        final int maxPrice = config.getInteger( MAX_PRICE_CONFIG_PARAMETER );
        this.tickerPriceGenerator = new TickerPriceGenerator( TickerGenerator.getTickersOrFail(), minPrice, maxPrice );
        this.tickerPriceGeneratorThread = new Thread( this.tickerPriceGenerator );
        this.tickerPriceGeneratorThread.start();
        this.tickerPrices = tickerPriceGenerator.getTickerPrices();
        this.j = tickersPerTimeUnit;
        this.tuplesPerInvocation = context.getConfig().getInteger( TUPLES_PER_INVOCATION_CONFIG_PARAMETER );

        return ScheduleWhenAvailable.INSTANCE;
    }

    @Override
    public final void invoke ( final InvocationContext context )
    {
        final Tuples output = context.getOutput();

        for ( int i = 0; i < tuplesPerInvocation; i++ )
        {
            output.add( nextTuple() );
        }
    }

    abstract Tuple nextTuple ();

    final long nextTimestamp ()
    {
        if ( j == 0 )
        {
            timestamp++;
            j = tickersPerTimeUnit;
        }
        else
        {
            j--;
        }

        return timestamp;
    }

    final Pair<String, Double> nextTickerPrice ()
    {
        if ( i == tickerPrices.size() )
        {
            tickerPrices = tickerPriceGenerator.getTickerPrices();
            i = 0;
        }

        return tickerPrices.get( i++ );
    }

    @Override
    public void shutdown ()
    {
        tickerPriceGenerator.shutdown( tickerPriceGeneratorThread );
    }

}
