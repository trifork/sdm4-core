package dk.nsi.sdm4.core.persistence.migration;

import org.apache.commons.io.IOUtils;
import org.springframework.core.io.Resource;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.io.Writer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Migration implements Comparable<Migration> {
	private String version;
	private String description;
	private Resource sqlSource;

	private static final Pattern migrationNamePattern = Pattern.compile(".*/?V(\\d{8}_\\d{4})__(.*).sql");
	private boolean valid;

	protected Migration(String version, String description, Resource sqlSource) {
		// used for testing
		this.version = version;
		this.description = description;
		this.sqlSource = sqlSource;
	}

	public Migration(Resource sqlSource) {
		this.sqlSource = sqlSource;
		parseResourcename();
	}

	private void parseResourcename() {
		Matcher matcher = migrationNamePattern.matcher(sqlSource.getFilename());
		if (matcher.find()) {
			valid = true;
			version = matcher.group(1);
			description = matcher.group(2);
		} else {
			valid = false;
		}
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

	public boolean isValid() {
		return valid;
	}

	@Override
	public String toString() {
		return "Migration[version=" + version + ", description=" + description + "]";
	}

	@Override
	public int compareTo(Migration other) {
			return this.getVersion().compareTo(other.getVersion());
	}
}
