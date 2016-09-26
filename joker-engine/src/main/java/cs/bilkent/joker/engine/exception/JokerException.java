package cs.bilkent.joker.engine.exception;

public class JokerException extends RuntimeException
{

    public JokerException ( final String msg )
    {
        super( msg );
    }

    public JokerException ( final String msg, final Exception cause )
    {
        super( msg, cause );
    }

}
