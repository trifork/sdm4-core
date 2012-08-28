package dk.nsi.sdm4.core.persistence.migration;

import org.apache.log4j.Logger;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MigrationFinder {
	private static final Logger log = Logger.getLogger(MigrationFinder.class);
	private static final String baseDir = "db/migration";
	private static final String sqlMigrationPrefix = "V";
	private static final String sqlMigrationSuffix = ".sql";

	public List<Migration> findMigrations() {
		ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

		List<Migration> migrations = new ArrayList<Migration>();

		if (!new ClassPathResource(baseDir + "/", classLoader).exists()) {
			log.warn("path for sql migrations not found: " + baseDir);
			return migrations;
		}

		Resource[] resources;
		try {
			String searchRoot = baseDir + "/";

			final String searchPattern = "**/" + sqlMigrationPrefix + "?*" + sqlMigrationSuffix;
			resources = new PathMatchingResourcePatternResolver(classLoader)
					.getResources("classpath:" + searchRoot + searchPattern);

			for (Resource resource : resources) {
				Migration migration = new Migration(resource);
				if (migration.isValid()) {
				migrations.add(migration);
				} else {
					log.warn("Ignoring resource " + resource.getDescription() + " as it's name does not match the migration name pattern");
				}
			}
		} catch (IOException e) {
			throw new DbMigratorException("Error loading sql testmigrations files", e);
		}

		Collections.sort(migrations);

		return migrations;
	}


}
