package dk.nsi.sdm4.core.status;

import org.joda.time.DateTime;

public interface TimeSource {
	DateTime now();
}
