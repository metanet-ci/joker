package cs.bilkent.joker.engine.config;

public enum ThreadingPref
{
    SINGLE_THREADED, MULTI_THREADED;

    public ThreadingPref reverse ()
    {
        return this == SINGLE_THREADED ? MULTI_THREADED : SINGLE_THREADED;
    }

}
