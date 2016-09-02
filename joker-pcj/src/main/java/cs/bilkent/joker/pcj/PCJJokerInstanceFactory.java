package cs.bilkent.joker.pcj;

import cs.bilkent.joker.Joker;
import cs.bilkent.joker.engine.migration.MigrationService;

public interface PCJJokerInstanceFactory
{

    Joker createJokerInstance ( Object jokerId, MigrationService migrationService );

}
