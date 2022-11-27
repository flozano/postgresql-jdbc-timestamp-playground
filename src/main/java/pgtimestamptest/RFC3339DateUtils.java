package pgtimestamptest;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public abstract class RFC3339DateUtils {

	private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ISO_DATE_TIME.withZone(ZoneOffset.UTC);

	public static String format(Instant instant) {
		return FORMATTER.format(instant);
	}

	public static String format(Date date) {
		return format(date.toInstant());
	}

	public static String format(Timestamp timestamp) {
		return format(timestamp.toInstant());
	}

	public static Instant parseInstant(String value) {
		return Instant.from(FORMATTER.parse(value));
	}

	public static OffsetDateTime parseOffsetDateTime(String value) {
		return OffsetDateTime.from(FORMATTER.parse(value));
	}

	public static Date parseDate(String value) {
		return new Date(parseInstant(value).toEpochMilli());
	}

	public static Timestamp parseTimestamp(String value) {
		return Timestamp.from(parseInstant(value));
	}

}
