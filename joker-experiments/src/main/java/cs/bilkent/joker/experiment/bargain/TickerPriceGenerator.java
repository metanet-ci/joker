package cs.bilkent.joker.experiment.bargain;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import cs.bilkent.joker.utils.Pair;
import static java.lang.Thread.currentThread;
import static java.util.Collections.shuffle;
import static java.util.concurrent.locks.LockSupport.parkNanos;

public class TickerPriceGenerator implements Runnable
{

    private final Random random = new Random();

    private final List<String> tickers;

    private final int minPrice;

    private final int maxPrice;

    private final AtomicReference<List<Pair<String, Double>>> tickerPricesRef = new AtomicReference<>();

    private volatile boolean running = true;

    public TickerPriceGenerator ( final Set<String> tickers, final int minPrice, final int maxPrice )
    {
        this.tickers = new ArrayList<>( tickers );
        this.minPrice = minPrice;
        this.maxPrice = maxPrice;
    }

    @Override
    public void run ()
    {
        while ( running )
        {
            setTickerPricesRef( generateTickerPrices() );
        }
    }

    private List<Pair<String, Double>> generateTickerPrices ()
    {
        final int tickerCount = tickers.size();
        final int count = 100;
        final List<Pair<String, Double>> tickerPrices = new ArrayList<>( tickerCount * count );
        final List<Double> prices = generatePrices();

        for ( int i = 0; i < count; i++ )
        {
            shuffle( tickers );
            shuffle( prices );

            for ( int j = 0; j < tickerCount; j++ )
            {
                tickerPrices.add( Pair.of( tickers.get( j ), prices.get( j ) ) );
            }
        }
        return tickerPrices;
    }

    private void setTickerPricesRef ( final List<Pair<String, Double>> tickerPrices )
    {
        while ( !tickerPricesRef.compareAndSet( null, tickerPrices ) )
        {
            if ( !running )
            {
                break;
            }

            parkNanos( 1000 );
        }
    }

    private List<Double> generatePrices ()
    {
        final List<Double> prices = new ArrayList<>( tickers.size() );
        for ( int j = 0; j < tickers.size(); j++ )
        {
            final int price = minPrice + random.nextInt( maxPrice - minPrice );
            prices.add( (double) price );
        }
        return prices;
    }

    public List<Pair<String, Double>> getTickerPrices ()
    {
        while ( true )
        {
            final List<Pair<String, Double>> tickerPrices = tickerPricesRef.get();
            if ( tickerPrices != null )
            {
                tickerPricesRef.compareAndSet( tickerPrices, null );
                return tickerPrices;
            }
        }
    }

    public void shutdown ( final Thread threadToJoin )
    {
        running = false;

        try
        {
            threadToJoin.join( TimeUnit.SECONDS.toMillis( 30 ) );
        }
        catch ( InterruptedException e )
        {
            currentThread().interrupt();
            throw new RuntimeException( e );
        }
    }

}
