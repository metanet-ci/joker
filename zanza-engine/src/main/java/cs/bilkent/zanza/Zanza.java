package cs.bilkent.zanza;

import com.google.inject.Guice;
import com.google.inject.Injector;

import cs.bilkent.zanza.engine.ZanzaEngine;
import cs.bilkent.zanza.engine.config.ZanzaConfig;

public class Zanza
{

    private final Injector injector;

    private final ZanzaEngine engine;

    public Zanza ( final ZanzaConfig config )
    {
        this.injector = Guice.createInjector( new ZanzaModule( config ) );
        this.engine = injector.getInstance( ZanzaEngine.class );
    }

}
