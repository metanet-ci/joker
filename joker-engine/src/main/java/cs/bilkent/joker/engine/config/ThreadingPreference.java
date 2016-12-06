package cs.bilkent.joker.engine.config;

public enum ThreadingPreference
{
    SINGLE_THREADED, MULTI_THREADED;

    public ThreadingPreference reverse ()
    {
        return this == SINGLE_THREADED ? MULTI_THREADED : SINGLE_THREADED;
    }

}
