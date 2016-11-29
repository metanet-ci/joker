package cs.bilkent.joker.engine.util.concurrent;

// copy of org.agrona.concurrent.IdleStrategy with a minor difference in the signature of idle() method.
public interface IdleStrategy
{

    boolean idle ();

    void reset ();

}
