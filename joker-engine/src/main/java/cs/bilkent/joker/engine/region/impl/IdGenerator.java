package cs.bilkent.joker.engine.region.impl;

import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Singleton;

@NotThreadSafe
@Singleton
public class IdGenerator
{

    private int next;

    public IdGenerator ()
    {
        this( 0 );
    }

    public IdGenerator ( final int next )
    {
        this.next = next;
    }

    int nextId ()
    {
        return next++;
    }

}
