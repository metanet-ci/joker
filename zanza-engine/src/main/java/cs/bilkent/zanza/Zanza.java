package cs.bilkent.zanza;

import java.util.concurrent.Future;

import com.google.inject.Guice;
import com.google.inject.Injector;

import cs.bilkent.zanza.engine.ZanzaEngine;
import cs.bilkent.zanza.engine.config.ZanzaConfig;
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
        this.injector = Guice.createInjector( new ZanzaModule( config ) );
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

}
