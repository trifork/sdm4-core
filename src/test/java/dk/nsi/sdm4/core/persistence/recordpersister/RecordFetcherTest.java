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
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;
import org.springframework.transaction.annotation.Transactional;

import java.sql.SQLException;
import java.util.List;

import static dk.nsi.sdm4.core.persistence.recordpersister.FieldSpecification.field;
import static org.junit.Assert.assertEquals;

@RunWith(SpringJUnit4ClassRunner.class)
@Transactional
@ContextConfiguration(loader = AnnotationConfigContextLoader.class)
public class RecordFetcherTest
{
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

	@Autowired
	private Instant transactionTime;

	@Autowired
    private RecordPersister persister;

	@Autowired
    private RecordFetcher fetcher;

	@Autowired
	private JdbcTemplate jdbcTemplate;

    @Before
    public void setup() throws SQLException
    {
        recordSpecification = RecordSpecification.createSpecification("SikredeTest", "Moo",
                field("Foo", 2).numerical(),
                field("Moo", 5)
        );
        
        createRecordFieldsTableOnDatabase(recordSpecification);
    }

    @Test
    public void testCorrectOrderWhenSameModifiedDate() throws SQLException
    {
        Record recordA = new RecordBuilder(recordSpecification).field("Foo", 42).field("Moo", "Far").build();
        Record recordB = new RecordBuilder(recordSpecification).field("Foo", 23).field("Moo", "Bar").build();
        
        persister.persist(recordA, recordSpecification);
        persister.persist(recordB, recordSpecification);

        List<RecordMetadata> result = fetcher.fetchSince(recordSpecification, 0L, transactionTime, 10);

        assertEquals(2, result.size());
        assertEquals(recordA, result.get(0).getRecord());
        assertEquals(recordB, result.get(1).getRecord());
    }

    @Test
    public void testCorrectOrderWhenSameModifiedDateButWithLimit() throws SQLException
    {
        Record recordA = new RecordBuilder(recordSpecification).field("Foo", 42).field("Moo", "Far").build();
        Record recordB = new RecordBuilder(recordSpecification).field("Foo", 23).field("Moo", "Bar").build();

        persister.persist(recordA, recordSpecification);
        persister.persist(recordB, recordSpecification);

        List<RecordMetadata> result = fetcher.fetchSince(recordSpecification, 0L, transactionTime, 1);

        assertEquals(1, result.size());
    }

    @Test
    public void testCorrectOrderWhenSameModifiedDateUsingLimitTwoCalls() throws SQLException
    {
        Record recordA = new RecordBuilder(recordSpecification).field("Foo", 42).field("Moo", "Far").build();
        Record recordB = new RecordBuilder(recordSpecification).field("Foo", 23).field("Moo", "Bar").build();

        persister.persist(recordA, recordSpecification);
        persister.persist(recordB, recordSpecification);

        List<RecordMetadata> result = fetcher.fetchSince(recordSpecification, 0L, transactionTime, 1);
        Long newPID = result.get(0).getPid();

        result = fetcher.fetchSince(recordSpecification, newPID, transactionTime, 1);
        assertEquals(1, result.size());
        assertEquals(recordB, result.get(0).getRecord());
    }

    @Test
    public void testLastRecordIsResentIfChanged() throws SQLException
    {
        Record recordA = new RecordBuilder(recordSpecification).field("Foo", 42).field("Moo", "Far").build();
        Record recordB = new RecordBuilder(recordSpecification).field("Foo", 23).field("Moo", "Bar").build();

        Instant year1995 = new DateTime(1995, 1, 1, 0, 0).toInstant();
        Instant year2000 = new DateTime(2000, 1, 1, 0, 0).toInstant();
        
        RecordPersister persisterYear1995 = new RecordPersister(year1995);
	    persisterYear1995.jdbcTemplate = jdbcTemplate;
        RecordPersister persisterYear2000 = new RecordPersister(year2000);
	    persisterYear2000.jdbcTemplate = jdbcTemplate;

	    persisterYear1995.persist(recordA, recordSpecification);
        persisterYear1995.persist(recordB, recordSpecification);

        List<RecordMetadata> result = fetcher.fetchSince(recordSpecification, 0L, year1995, 10);
        assertEquals(2, result.size());
        RecordMetadata lastRecordMetadata = result.get(1);

        persisterYear2000.persist(recordB, recordSpecification);
        result = fetcher.fetchSince(recordSpecification, lastRecordMetadata.getPid(), lastRecordMetadata.getModifiedDate(), 10);
        assertEquals(1, result.size());
        assertEquals(recordB, result.get(0).getRecord());
    }

    @Test
    public void testAdvancedResend() throws SQLException
    {
        Record recordA = new RecordBuilder(recordSpecification).field("Foo", 1).field("Moo", "Far").build();
        Record recordB = new RecordBuilder(recordSpecification).field("Foo", 2).field("Moo", "Bar").build();
        Record recordC = new RecordBuilder(recordSpecification).field("Foo", 3).field("Moo", "Start").build();
        Record recordD = new RecordBuilder(recordSpecification).field("Foo", 4).field("Moo", "End").build();

        Instant year1995 = new DateTime(1995, 1, 1, 0, 0).toInstant();
        Instant year2000 = new DateTime(2000, 1, 1, 0, 0).toInstant();

        RecordPersister persisterYear1995 = new RecordPersister(year1995);
	    persisterYear1995.jdbcTemplate = jdbcTemplate;
	    RecordPersister persisterYear2000 = new RecordPersister(year2000);
	    persisterYear2000.jdbcTemplate = jdbcTemplate;


	    persisterYear1995.persist(recordA, recordSpecification);
        persisterYear1995.persist(recordB, recordSpecification);
        persisterYear1995.persist(recordC, recordSpecification);

        List<RecordMetadata> result = fetcher.fetchSince(recordSpecification, 0L, year1995, 10);
        assertEquals(3, result.size());
        RecordMetadata lastRecordMetadata = result.get(2);

        persisterYear2000.persist(recordB, recordSpecification);
        persisterYear2000.persist(recordC, recordSpecification);
        persisterYear2000.persist(recordD, recordSpecification);
        result = fetcher.fetchSince(recordSpecification, lastRecordMetadata.getPid(), lastRecordMetadata.getModifiedDate(), 10);
        assertEquals(3, result.size());
        assertEquals(recordB, result.get(0).getRecord());
        assertEquals(recordC, result.get(1).getRecord());
        assertEquals(recordD, result.get(2).getRecord());
    }

    private void createRecordFieldsTableOnDatabase(RecordSpecification recordSpecification) throws SQLException
    {
        jdbcTemplate.update("DROP TABLE IF EXISTS " + recordSpecification.getTable());
	    jdbcTemplate.update(RecordMySQLTableGenerator.createSqlSchema(recordSpecification));
    }
}
