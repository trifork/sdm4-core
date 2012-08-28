package dk.nsi.sdm4.core.persistence.migration;

import org.apache.commons.io.IOUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;

import java.io.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Migration {
	private String version;
	private String description;
	private Resource sqlSource;

	private static final Pattern migrationNamePattern = Pattern.compile(".*/?V(\\d{8}_\\d{4})__(.*).sql");

	public Migration(Resource sqlSource) {
		this.sqlSource = sqlSource;
		parseResourcename();
	}

	private void parseResourcename() {
		Matcher matcher = migrationNamePattern.matcher(sqlSource.getFilename());
		matcher.find();
		version = matcher.group(1);
		description = matcher.group(2);
	}

	public String getVersion() {
		return version;
	}

	public String getDescription() {
		return description;
	}

	public String getSql() {
		InputStream input = null;
		Writer output = null;
		try {
			input = sqlSource.getInputStream();
			output = new StringWriter();
			IOUtils.copy(input, output, "UTF-8");
		} catch (IOException e) {
			throw new DbMigratorException("Could not get sql from resource " + sqlSource.getDescription(),e);
		} finally {
			IOUtils.closeQuietly(input);
			IOUtils.closeQuietly(output);
		}

		return output.toString();
	}
}
