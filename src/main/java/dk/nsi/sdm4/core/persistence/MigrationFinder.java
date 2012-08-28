package dk.nsi.sdm4.core.persistence;

import dk.nsi.sdm4.core.persistence.migration.DbMigratorException;
import dk.nsi.sdm4.core.persistence.migration.Migration;
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
				migrations.add(new Migration(resource));
			}
		} catch (IOException e) {
			throw new DbMigratorException("Error loading sql testmigrations files", e);
		}

		Collections.sort(migrations);

		return migrations;
	}


}
