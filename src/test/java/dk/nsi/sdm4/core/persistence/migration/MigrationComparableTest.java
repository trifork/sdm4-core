package dk.nsi.sdm4.core.persistence.migration;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class MigrationComparableTest {
	private Migration migration2;
	private Migration migration1;
	private Migration migration3;
	private Migration migrationTime1;
	private Migration migrationTime2;

	@Before
	public void setup() {
		migration1 = new Migration("20120728_0101", null, null);
		migration2 = new Migration("20120828_0101", null, null);
		migration3 = new Migration("20120901_0101", null, null);

		migrationTime1 = new Migration("20120901_0101", null, null);
		migrationTime2 = new Migration("20120901_0102", null, null);
	}


	@Test
	public void comparesDifferentMonthsCorrectly() {
		assertTrue(migration1.compareTo(migration2) < 0);
	}

	@Test
	public void doesNotJustUseTheDayOfMonth() {
		assertTrue(migration2.compareTo(migration3) < 0);
	}

	@Test
	public void canCrossSeveralMonths() {
		assertTrue(migration3.compareTo(migration1) > 0);
	}

	@Test
	public void understandsTimestampPartOfVersion() {
		assertTrue(migrationTime1.compareTo(migrationTime2) < 0);
	}
}
