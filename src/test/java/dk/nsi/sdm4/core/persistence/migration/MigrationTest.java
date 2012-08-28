package dk.nsi.sdm4.core.persistence.migration;

import org.junit.Test;
import org.springframework.core.io.AbstractResource;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

import java.io.IOException;
import java.io.InputStream;

import static org.junit.Assert.assertEquals;

public class MigrationTest {
	@Test
	public void willParseResourceNameCorrectly() {
		Resource testResource = new AbstractResource() {
			@Override
			public String getDescription() {
				throw new UnsupportedOperationException("getDescription");
			}

			@Override
			public InputStream getInputStream() throws IOException {
				throw new UnsupportedOperationException("getInputStream");
			}

			@Override
			public String getFilename() throws IllegalStateException {
				return "/a/b/c/V19999999_8888__Description7777.sql";
			}

			@Override
			public String toString() {
				return "TestResource";
			}
		};
		Migration migration = new Migration(testResource);

		assertEquals("19999999_8888", migration.getVersion());
		assertEquals("Description7777", migration.getDescription());
	}

	@Test
	public void willGetTheContentsOfResource() {
		Migration migration = new Migration(new ClassPathResource("V20000000_0000__MigrationTestSampleResource.sql"));
		assertEquals("Æblegrød med ål", migration.getSql());
	}
}
