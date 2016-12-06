package cs.bilkent.joker.engine.util;

public final class ExceptionUtils
{

    public static void checkInterruption ( final Exception e )
    {
        if ( e instanceof InterruptedException || e.getCause() instanceof InterruptedException )
        {
            Thread.currentThread().interrupt();
        }
    }

    private ExceptionUtils ()
    {

    }

}
