package cs.bilkent.joker.pcj;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.joker.Joker;
import cs.bilkent.joker.engine.migration.MigrationService;

public class JokerRegistry
{

    private static final JokerRegistry INSTANCE = new JokerRegistry();

    public static JokerRegistry getInstance ()
    {
        return INSTANCE;
    }

    private final AtomicReference<PCJJokerInstanceFactory> jokerFactoryRef = new AtomicReference<>();

    private final ConcurrentMap<Object, Joker> jokerInstances = new ConcurrentHashMap<>();

    private JokerRegistry ()
    {
    }

    public void start ( final PCJJokerInstanceFactory pcjJokerFactory )
    {
        checkArgument( jokerFactoryRef.compareAndSet( null, pcjJokerFactory ),
                       "%s is already set!",
                       PCJJokerInstanceFactory.class.getSimpleName() );
    }

    public Joker createJokerInstance ( final Object jokerId, final MigrationService migrationService ) throws InterruptedException
    {
        while ( jokerFactoryRef.get() == null )
        {
            Thread.sleep( 1 );
        }

        return jokerInstances.computeIfAbsent( jokerId, o -> jokerFactoryRef.get().createJokerInstance( jokerId, migrationService ) );
    }

    public void destroyJokerInstance ( final Object jokerId )
    {
        final Joker joker = jokerInstances.remove( jokerId );
        checkArgument( joker != null, "Joker instance %s to destroy not found!", jokerId );
    }

    public Collection<Joker> getJokerInstances ()
    {
        return new ArrayList<>( jokerInstances.values() );
    }

}
