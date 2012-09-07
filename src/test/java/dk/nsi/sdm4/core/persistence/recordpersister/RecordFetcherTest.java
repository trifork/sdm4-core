/**
 * The contents of this file are subject to the Mozilla Public
 * License Version 1.1 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of
 * the License at http://www.mozilla.org/MPL/
 *
 * Software distributed under the License is distributed on an "AS
 * IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * rights and limitations under the License.
 *
 * Contributor(s): Contributors are attributed in the source code
 * where applicable.
 *
 * The Original Code is "Stamdata".
 *
 * The Initial Developer of the Original Code is Trifork Public A/S.
 *
 * Portions created for the Original Code are Copyright 2011,
 * Lægemiddelstyrelsen. All Rights Reserved.
 *
 * Portions created for the FMKi Project are Copyright 2011,
 * National Board of e-Health (NSI). All Rights Reserved.
 */
package dk.nsi.sdm4.core.persistence.recordpersister;

import org.joda.time.DateTime;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;
import org.springframework.transaction.annotation.Transactional;

import java.sql.SQLException;

import static dk.nsi.sdm4.core.persistence.recordpersister.FieldSpecification.field;
import static org.junit.Assert.assertEquals;

@RunWith(SpringJUnit4ClassRunner.class)
@Transactional
@ContextConfiguration(loader = AnnotationConfigContextLoader.class)
public class RecordFetcherTest {
	@Configuration
	@PropertySource("classpath:test.properties")
	@Import(RecordPersisterTestDatasourceConfiguration.class)
	static class ContextConfiguration {
		@Bean
		public RecordFetcher recordFetcher() {
			return new RecordFetcher();
		}

		@Bean
		public RecordPersister persister(Instant transactionTime) {
			return new RecordPersister(transactionTime);
		}

		@Bean
		public Instant transactionTime() {
			return new DateTime(2011, 5, 29, 0, 0, 0).toInstant();
		}
	}

	private RecordSpecification recordSpecification;
	private RecordSpecification decimalRecordSpec;

	@Autowired
	private RecordPersister persister;

	@Autowired
	private RecordFetcher fetcher;

	@Autowired
	private JdbcTemplate jdbcTemplate;

	@Before
	public void setup() throws SQLException {
		decimalRecordSpec = RecordSpecification.createSpecification("SikredeTestDecimal", "Moo",
				field("Foo", 2).decimal10_3(),
				field("Moo", 5)
		);

		recordSpecification = RecordSpecification.createSpecification("SikredeTest", "Moo",
				field("Foo", 2).numerical(),
				field("Moo", 5)
		);

		createRecordFieldsTableOnDatabase();
	}

	@Test(expected = IncorrectResultSizeDataAccessException.class)
	public void testFetchCurrentThrowsExceptionWhenTwoRecordsHasValidToNull() throws SQLException {
		Record recordA = new RecordBuilder(recordSpecification).field("Foo", 42).field("Moo", "Far").build();

		persister.persist(recordA, recordSpecification);
		persister.persist(recordA, recordSpecification);

		fetcher.fetchCurrent("Far", recordSpecification);
	}

	@Test
	public void testFetchCurrentReturnsSingletonRecord() throws SQLException {
		Record recordA = new RecordBuilder(recordSpecification).field("Foo", 42).field("Moo", "Far").build();

		persister.persist(recordA, recordSpecification);

		Record dbRecord = fetcher.fetchCurrent("Far", recordSpecification);
		assertEquals(recordA, dbRecord);
	}

	@Test
	public void testFetchDecimal10_3() throws SQLException
	{
		Record recordA = new RecordBuilder(decimalRecordSpec).field("Foo", 42.2).field("Moo", "Far").build();

		persister.persist(recordA, decimalRecordSpec);

		Record result = fetcher.fetchCurrent("Far", decimalRecordSpec);
		assertEquals(42.2, result.get("Foo"));
	}

	private void createRecordFieldsTableOnDatabase() throws SQLException {
		jdbcTemplate.update("DROP TABLE IF EXISTS " + recordSpecification.getTable());
		jdbcTemplate.update(RecordMySQLTableGenerator.createSqlSchema(recordSpecification));

		jdbcTemplate.update("DROP TABLE IF EXISTS " + decimalRecordSpec.getTable());
		jdbcTemplate.update(RecordMySQLTableGenerator.createSqlSchema(decimalRecordSpec));
	}
}
