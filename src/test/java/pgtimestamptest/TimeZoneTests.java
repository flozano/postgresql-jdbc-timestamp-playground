package pgtimestamptest;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.Month;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.SimpleDriverDataSource;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.github.freva.asciitable.AsciiTable;

@Testcontainers
class TimeZoneTests {

	@Container
	private PostgreSQLContainer postgresqlContainer = (PostgreSQLContainer) new PostgreSQLContainer(
			"postgres:15-alpine").withDatabaseName("pruebas").withUsername("pruebas").withPassword("pruebas")
			.withEnv("TZ", "UTC");
	private SimpleDriverDataSource ds;
	private DataSourceTransactionManager tm;
	private TransactionTemplate transactions;
	private NamedParameterJdbcTemplate jdbc;

	@BeforeEach
	void createDataSource() {
		ds = setupDataSource(postgresqlContainer);
		tm = new DataSourceTransactionManager(ds);
		transactions = new TransactionTemplate(tm);
		jdbc = new NamedParameterJdbcTemplate(ds);
		ensureDatabaseWorks(transactions, jdbc);
	}

	@AfterEach
	void destroyDataSource() {
		ds = null;
	}

	@Test
	void to_charTimestampBehaviour() {
		TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
		String dateString = "'2022-11-17T05:00:00.000'";
		transactions.execute(t -> {
			jdbc.update("SET LOCAL TIME ZONE 'America/Toronto'", Map.of());
			List<String> parts = List.of("current_setting('TIMEZONE')", //
					dateString + "::timestamp::text", //
					"(" + dateString + "::timestamp at time zone 'UTC')::text", //
					"to_char((" + dateString + "::timestamp at time zone 'UTC'), 'YYYY-MM-DD HH24:MI:SS.MS TZ' )" //
			);
			var sql = "SELECT " + parts.stream().collect(Collectors.joining(", "));
			jdbc.query(sql, Map.of(), new RowCallbackHandler() {

				@Override
				public void processRow(ResultSet rs) throws SQLException {
					for (int i = 0; i < parts.size(); i++) {
						System.err.println(parts.get(i) + " -> " + rs.getObject(i + 1));
					}
				}
			});
			return null;
		});
	}

	@Test
	void timestampBehaviourWithTimestamptz() {
		String dateString = "2022-11-17T00:00:00.000Z";
		var instant = RFC3339DateUtils.parseInstant(dateString);
		transactions.execute(t -> {
			jdbc.query("SELECT '" + dateString + "'::timestamptz as f1", Map.of(), new RowCallbackHandler() {
				@Override
				public void processRow(ResultSet rs) throws SQLException {
					assertEquals(instant.toEpochMilli(), rs.getTimestamp(1).getTime(), //
							"Timestamptz works as expected when no calendar is passed");
					assertEquals(instant.toEpochMilli(),
							rs.getTimestamp(1, Calendar.getInstance(TimeZone.getTimeZone("UTC"))).getTime(),
							"Timestamptz works as expected when UTC calendar is passed");
					assertEquals(instant.toEpochMilli(),
							rs.getTimestamp(1, Calendar.getInstance(TimeZone.getTimeZone("Asia/Tokyo"))).getTime(),
							"Calendar passed to ResultSet#getTimestamp does not affect result when using timestamptz");
				}
			});
			return null;
		});
	}

	@Test
	void date_truncWithPgTimestamp() {
		String dateString = "2022-11-17T00:00:00.000";
		var instant = RFC3339DateUtils.parseInstant(dateString + "Z");
		var targetTimeZone = TimeZone.getTimeZone("Asia/Tokyo");
		var systemTimeZone = TimeZone.getDefault();
		var utcTimeZone = TimeZone.getTimeZone("UTC");

		transactions.execute(t -> {
			jdbc.query("SELECT date_trunc('day', '" + dateString + "'::timestamp) as f1", Map.of(),
					new RowCallbackHandler() {
						@Override
						public void processRow(ResultSet rs) throws SQLException {
							var localDate = rs.getObject(1, LocalDate.class);
							assertEquals(LocalDate.of(2022, Month.NOVEMBER, 17), localDate);
							var ts = rs.getTimestamp(1, Calendar.getInstance(utcTimeZone));
							assertEquals(instant.toEpochMilli(), ts.getTime());
						}
					});
			return null;
		});
	}

	@Test
	void timestampBehaviourWithPgTimestamp() {
		String dateString = "2022-11-17T00:00:00.000";
		var instant = RFC3339DateUtils.parseInstant(dateString + "Z");
		var targetTimeZone = TimeZone.getTimeZone("Asia/Tokyo");
		var systemTimeZone = TimeZone.getDefault();

		transactions.execute(t -> {
			jdbc.query("SELECT '" + dateString + "'::timestamp as f1", Map.of(), new RowCallbackHandler() {
				@Override
				public void processRow(ResultSet rs) throws SQLException {
					assertEquals(instant.toEpochMilli(),
							rs.getTimestamp(1, Calendar.getInstance(TimeZone.getTimeZone("UTC"))).getTime(), //
							"Timestamp fields stored as UTC will work as expected when passing UTC");

					assertEquals(instant.toEpochMilli() - targetTimeZone.getOffset(instant.toEpochMilli()),
							rs.getTimestamp(1, Calendar.getInstance(TimeZone.getTimeZone("Asia/Tokyo"))).getTime(), //
							"Timestamp fields are shifted to specific time-zone if a specific calendar is passed");

					assertEquals(instant.toEpochMilli() - systemTimeZone.getOffset(instant.toEpochMilli()),
							rs.getTimestamp(1).getTime(), //
							"Timestamp fields are shifted to local-time-zone if no calendar is passed; JVM's time-zone is used");
				}
			});
			return null;
		});
	}

	@Test
	void timezoneDateTruncTable() {
		System.err.println("Default TZ: " + ZoneId.systemDefault());
		var staticFields = List.of("'i'", "'rfc3339 date'", "'OffsetDateTime'");
		var sqlFields = List.of("DATE_TRUNC('day', :t)", ":t::varchar", "pg_typeof(DATE_TRUNC('day', :t))",
				" DATE_TRUNC('day', :t)::varchar");
		var sql = "SELECT " + sqlFields.stream().collect(Collectors.joining(", "));
		var header = Stream.concat(staticFields.stream(), sqlFields.stream()).toArray(String[]::new);
		for (var tz : List.of("Europe/Madrid", "Asia/Tokyo", "GMT")) {
			System.err.println(tz + ": ");
			List<Object[]> rows = new ArrayList<>();
			for (int i = -9; i < 9; i++) {
				var rfcDateTime = "2022-11-17T00:00:00.000" + (i < 0 ? "-" : "+") + "0" + Math.abs(i) + ":00";
				var offsetDateTime = RFC3339DateUtils.parseOffsetDateTime(rfcDateTime);

				Object[] sqlRow = transactions.execute(transactionStatus -> {
					jdbc.update("SET LOCAL TIME ZONE '" + tz + "'", Map.of());
					return jdbc.queryForObject(sql, Map.of("t", offsetDateTime), (rs, rn) -> {
						var result = new Object[sqlFields.size()];
						for (int j = 0; j < result.length; j++) {
							result[j] = rs.getObject(j + 1);
						}
						return result;
					});
				});
				rows.add(concatWithArrayCopy(new Object[] { i, rfcDateTime, offsetDateTime }, sqlRow));
			}
			System.err.println(AsciiTable.getTable(header, rows.toArray(x -> new Object[x][])));

		}
	}

	static <T> T[] concatWithArrayCopy(T[] array1, T[] array2) {
		T[] result = Arrays.copyOf(array1, array1.length + array2.length);
		System.arraycopy(array2, 0, result, array1.length, array2.length);
		return result;
	}

	private static SimpleDriverDataSource setupDataSource(PostgreSQLContainer postgresqlContainer) {
		var ds = new SimpleDriverDataSource(postgresqlContainer.getJdbcDriverInstance(),
				postgresqlContainer.getJdbcUrl());
		ds.setUsername("pruebas");
		ds.setPassword("pruebas");
		return ds;
	}

	private static void ensureDatabaseWorks(TransactionTemplate transactions, NamedParameterJdbcTemplate jdbc) {
		assertEquals(1, (int) transactions.execute((t) -> jdbc.queryForObject("SELECT 1", Map.of(), Integer.class)));
	}
}
