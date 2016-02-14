package cs.bilkent.zanza.engine;

public class TestUtils
{

    public static Thread spawnThread ( final Runnable runnable )
    {
        final Thread thread = new Thread( runnable );
        thread.start();
        return thread;
    }

}
