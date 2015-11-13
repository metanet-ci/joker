package cs.bilkent.zanza.engine;

import com.google.inject.AbstractModule;

public class EngineModule extends AbstractModule
{
    @Override
    protected void configure ()
    {
        bind( ClassA.class );
        bind( ClassB.class );
        bind( ClassC.class );
    }
}
