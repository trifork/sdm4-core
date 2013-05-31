package dk.nsi.sdm4.core.parser;

import dk.nsi.sdm4.core.persistence.recordpersister.RecordFetcher;
import dk.nsi.sdm4.core.persistence.recordpersister.RecordPersister;
import dk.nsi.sdm4.core.status.ImportStatusRepository;
import dk.sdsd.nsp.slalog.api.SLALogItem;
import dk.sdsd.nsp.slalog.api.SLALogger;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.Instant;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.transaction.annotation.Transactional;

import java.io.*;
import java.security.MessageDigest;

/**
 * Ansvarlig for at foretage jævnlige kørsler af én importer samt at skaffe den inddata.
 */
public class ParserExecutor {
	@Autowired
	Parser parser;

	@Autowired
	Inbox inbox;

	@Autowired
	ImportStatusRepository importStatusRepo;

	@Autowired
	RecordPersister recordPersister;

    @Autowired
    RecordFetcher recordFetcher;

	@Autowired
	private SLALogger slaLogger;

	protected static Logger logger = Logger.getLogger(ParserExecutor.class); // we need to be able to test the logging behaviour, therefore this field is not private and not final

	@Scheduled(fixedDelay = 1000)
	@Transactional
	/**
	 * Metode, der når inbox ikke er låst og indeholder et dataset, kalder sin importer.
	 * Håndterer SLA-logning for hele importen og etablerer en transaktion, importer kører i.
	 */
	public void run() {
        // We are using the SLA logger for parsers aswell
        // To tie runs together we create an identifier that add as message id to the sla log
        String parserIdentifier = parser.getHome();
        String runIdentifier = parserIdentifier + "-" + Instant.now().getMillis();
		SLALogItem slaLogItem = slaLogger.createLogItem(parserIdentifier+".ParserExecutor", "SDM4CORE_ENTRY");
        slaLogItem.setMessageId(runIdentifier);
        try {
			if (!inbox.isLocked()) {
				if (logger.isDebugEnabled()) {
					logger.debug("Running parser " + parser.getHome());
				}

				inbox.update();
				File dataSet = inbox.top();

				if (dataSet != null) {
					logDatasetContents(dataSet);
                    // Make sure persister and fetcher uses exact same time
					recordPersister.resetTransactionTime();
                    recordFetcher.setTransactionTime(recordPersister.getTransactionTime());

					importStatusRepo.importStartedAt(new DateTime());
					parser.process(dataSet, runIdentifier);

					// Once the import is complete
					// we can remove the data set
					// from the inbox.
					inbox.advance();

					slaLogItem.setCallResultOk();
					slaLogItem.store();

					importStatusRepo.importEndedWithSuccess(new DateTime());
				} // if there is no data and no error, we never call store on the log item, which is okay
			} else {
				if (logger.isDebugEnabled()) {
					logger.debug(inbox + " for parser " + parser.getHome() + " is locked");
				}
			}
		} catch (Exception e) {
			try {
				inbox.lock();
			} catch (RuntimeException lockExc) {
				logger.error("Unable to lock " + inbox, lockExc);
			}

			slaLogItem.setCallResultError("Parser " + parserIdentifier + " failed - Cause: " + e.getMessage());
			slaLogItem.store();

			importStatusRepo.importEndedWithFailure(new DateTime());

			throw new RuntimeException("runParserOnInbox on parser " + parserIdentifier + " failed", e); // to make sure the transaction rolls back
		}
	}

	private void logDatasetContents(File dataSet) {
		StringBuilder message = new StringBuilder("Begin processing of dataset, datasetDir=").append(dataSet.getAbsoluteFile());
		for (File file : dataSet.listFiles()) {
			message.append(", file=").append(file.getAbsolutePath());
			message.append(", md5=").append(createMd5Checksum(file.getAbsolutePath()));
		}

		logger.info(message.toString());
	}

    /**
     * Calculate md5 checksum of a file.
     * @param filename
     * @return
     */
    private String createMd5Checksum(String filename) {
        String md5 = "";
        try {
            InputStream inputStream = new FileInputStream(filename);
            byte[] buffer = new byte[16384];

            MessageDigest digester = MessageDigest.getInstance("MD5");
            int readden;
            while ((readden = inputStream.read(buffer)) != -1) {
                digester.update(buffer, 0, readden);
            }
            inputStream.close();
            byte[] digest = digester.digest();
            md5 = String.valueOf(Hex.encodeHex(digest));
        } catch (Exception e) {
            logger.warn("Failed to calculate MD5 for file " + filename, e);
        }
        return md5;
    }
}
