package cs.bilkent.joker.engine.exception;

public class InitializationException extends RuntimeException
{

    public InitializationException ( final String msg )
    {
        super( msg );
    }

    public InitializationException ( final String msg, final Throwable cause )
    {
        super( msg, cause );
    }

}
