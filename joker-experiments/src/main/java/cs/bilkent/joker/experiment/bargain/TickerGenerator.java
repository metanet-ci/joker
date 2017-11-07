package cs.bilkent.joker.experiment.bargain;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Collections.unmodifiableSet;

public final class TickerGenerator
{

    private static volatile Set<String> tickers;

    public static synchronized void init ( final int tickerCount )
    {
        checkState( tickers == null );

        char[] letters = { 'a',
                           'b',
                           'c',
                           'd',
                           'e',
                           'f',
                           'g',
                           'h',
                           'i',
                           'j',
                           'k',
                           'l',
                           'm',
                           'n',
                           'o',
                           'p',
                           'q',
                           'r',
                           's',
                           't',
                           'u',
                           'v',
                           'w',
                           'x',
                           'y',
                           'z' };

        final Set<String> tickers = new HashSet<>();
        final Random random = new Random();

        while ( tickers.size() < tickerCount )
        {
            StringBuilder sb = new StringBuilder();
            for ( int i = 0; i < 3; i++ )
            {
                sb.append( letters[ random.nextInt( letters.length ) ] );
            }

            tickers.add( sb.toString() );
        }

        TickerGenerator.tickers = unmodifiableSet( tickers );
    }

    public static Set<String> getTickersOrFail ()
    {
        checkState( tickers != null );
        return tickers;
    }

    private TickerGenerator ()
    {

    }

}
