package dk.nsi.sdm4.core.persistence.migration;

public class DbMigratorException extends RuntimeException {
	public DbMigratorException(String s, Exception cause) {
		super(s, cause);
	}
}
