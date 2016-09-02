package cs.bilkent.joker;

import java.util.UUID;
import java.util.concurrent.Future;

import com.google.inject.Guice;
import com.google.inject.Injector;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import cs.bilkent.joker.engine.FlowStatus;
import cs.bilkent.joker.engine.JokerEngine;
import cs.bilkent.joker.engine.config.JokerConfig;
import cs.bilkent.joker.engine.region.RegionConfigFactory;
import cs.bilkent.joker.flow.FlowDef;

public class Joker
{

    private final JokerEngine engine;

    private final Injector injector;

    public Joker ()
    {
        this( new JokerConfig() );
    }

    public Joker ( final JokerConfig config )
    {
        this( UUID.randomUUID().toString(), config, null );
    }

    private Joker ( final Object jokerId, final JokerConfig config, final RegionConfigFactory regionConfigFactory )
    {
        this.injector = Guice.createInjector( new JokerModule( jokerId, config, regionConfigFactory ) );
        this.engine = injector.getInstance( JokerEngine.class );
    }

    public void run ( final FlowDef flow )
    {
        engine.run( flow );
    }

    public FlowStatus getStatus ()
    {
        return engine.getStatus();
    }

    public Future<Void> shutdown ()
    {
        return engine.shutdown();
    }

    public static class JokerBuilder
    {

        private Object jokerId = UUID.randomUUID().toString();

        private JokerConfig jokerConfig = new JokerConfig();

        private RegionConfigFactory regionConfigFactory;

        private boolean built;

        public JokerBuilder ()
        {
        }

        public JokerBuilder ( final JokerConfig jokerConfig )
        {
            this.jokerConfig = jokerConfig;
        }

        public JokerBuilder setJokerConfig ( final JokerConfig jokerConfig )
        {
            checkArgument( jokerConfig != null );
            checkState( !built, "Joker is already built!" );
            this.jokerConfig = jokerConfig;
            return this;
        }

        public JokerBuilder setRegionConfigFactory ( final RegionConfigFactory regionConfigFactory )
        {
            checkArgument( regionConfigFactory != null );
            checkState( !built, "Joker is already built!" );
            this.regionConfigFactory = regionConfigFactory;
            return this;
        }

        public JokerBuilder setJokerId ( final Object jokerId )
        {
            checkArgument( jokerId != null );
            this.jokerId = jokerId;
            return this;
        }

        public Joker build ()
        {
            checkState( !built, "Joker is already built!" );
            built = true;
            return new Joker( jokerId, jokerConfig, regionConfigFactory );
        }

    }

}
