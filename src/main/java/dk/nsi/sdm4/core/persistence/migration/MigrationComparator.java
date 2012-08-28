package dk.nsi.sdm4.core.persistence.migration;

import java.util.Comparator;

class MigrationComparator implements Comparator<Migration> {
	@Override
	public int compare(Migration migration, Migration other) {
		return migration.getVersion().compareTo(other.getVersion());
	}
}
