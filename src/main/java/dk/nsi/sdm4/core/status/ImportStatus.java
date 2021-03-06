package dk.nsi.sdm4.core.status;

import org.joda.time.DateTime;
import org.joda.time.Duration;

public class ImportStatus {
	private DateTime startTime;
	private DateTime endTime;
	private Outcome outcome;

	public DateTime getStartTime() {
		return startTime;
	}

	public void setStartTime(DateTime startTime) {
		this.startTime = startTime;
	}

	public DateTime getEndTime() {
		return endTime;
	}

	public void setEndTime(DateTime endTime) {
		this.endTime = endTime;
	}

	public Outcome getOutcome() {
		return outcome;
	}

	public void setOutcome(Outcome outcome) {
		this.outcome = outcome;
	}

	public static enum Outcome {
		SUCCESS,
		FAILURE
	}

	@Override
	public String toString() {
		String body = "\nLast import started at: " + this.getStartTime();
		Outcome outcome = this.getOutcome();

		if (endTime != null) {
			body += " and ended at: " + this.getEndTime();
			body += ". Processing took " + new Duration(this.getStartTime(), this.getEndTime()).getStandardSeconds() + " seconds";
		} else {
			body += " and is still running";
		}

		if (outcome != null) {
			body += ". Outcome was " + outcome;
		}

		return body;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		ImportStatus that = (ImportStatus) o;

		if (endTime != null ? !endTime.equals(that.endTime) : that.endTime != null) return false;
		if (outcome != that.outcome) return false;
		if (startTime != null ? !startTime.equals(that.startTime) : that.startTime != null) return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = startTime != null ? startTime.hashCode() : 0;
		result = 31 * result + (endTime != null ? endTime.hashCode() : 0);
		result = 31 * result + (outcome != null ? outcome.hashCode() : 0);
		return result;
	}
}
