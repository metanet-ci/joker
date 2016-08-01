package cs.bilkent.zanza;

import java.util.concurrent.Future;

import com.google.inject.Guice;
import com.google.inject.Injector;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import cs.bilkent.zanza.engine.ZanzaEngine;
import cs.bilkent.zanza.engine.config.ZanzaConfig;
import cs.bilkent.zanza.engine.region.RegionConfigFactory;
import cs.bilkent.zanza.flow.FlowDef;

public class Zanza
{

    private final ZanzaEngine engine;

    private final Injector injector;

    public Zanza ()
    {
        this( new ZanzaConfig() );
    }

    public Zanza ( final ZanzaConfig config )
    {
        this( config, null );
    }

    private Zanza ( final ZanzaConfig config, final RegionConfigFactory regionConfigFactory )
    {
        this.injector = Guice.createInjector( new ZanzaModule( config, regionConfigFactory ) );
        this.engine = injector.getInstance( ZanzaEngine.class );
    }

    public void start ( final FlowDef flow )
    {
        engine.start( flow );
    }

    public Future<Void> shutdown ()
    {
        return engine.shutdown();
    }

    public static class ZanzaBuilder
    {

        private ZanzaConfig zanzaConfig = new ZanzaConfig();

        private RegionConfigFactory regionConfigFactory;

        private boolean built;

        public ZanzaBuilder ()
        {
        }

        public ZanzaBuilder ( final ZanzaConfig zanzaConfig )
        {
            this.zanzaConfig = zanzaConfig;
        }

        public ZanzaBuilder setZanzaConfig ( final ZanzaConfig zanzaConfig )
        {
            checkArgument( zanzaConfig != null );
            checkState( !built, "Zanza is already built!" );
            this.zanzaConfig = zanzaConfig;
            return this;
        }

        public ZanzaBuilder setRegionConfigFactory ( final RegionConfigFactory regionConfigFactory )
        {
            checkArgument( regionConfigFactory != null );
            checkState( !built, "Zanza is already built!" );
            this.regionConfigFactory = regionConfigFactory;
            return this;
        }

        public Zanza build ()
        {
            checkState( !built, "Zanza is already built!" );
            built = true;
            return new Zanza( zanzaConfig, regionConfigFactory );
        }

    }

}
