package dk.nsi.sdm4.core.persistence.migration;

import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class MigrationFinderTest {
	@Test
	public void willFindTwoMigrationsOnTheClasspath() {
		MigrationFinder migrationFinder = new MigrationFinder();
		List<Migration> migrations = migrationFinder.findMigrations();

		assertEquals(2, migrations.size());

		Collections.sort(migrations, new MigrationComparator());

		assertEquals("20010101_0101", migrations.get(0).getVersion());
		assertEquals("20010102_0101", migrations.get(1).getVersion());
	}
}
