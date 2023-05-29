package com.khaled.jdbc;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

import java.util.List;
import java.util.Properties;

import java.util.logging.LogManager;

import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import static java.util.stream.Collectors.toList;

/**
 * @author khaled
 *
 */
public class App {
    private static final System.Logger LOGGER = System.getLogger(App.class.getName());

    private static final String PROPERTIES_PATH = "application.properties";

    private static final String PREPARED_STATEMENT = "sql/queries.properties";

    private static final String LOGGING_PATH = "logging.properties";

    private static final String INIT_SQL_PATH = "sql/init.sql";

    private static final String DATA_LOAD_PATH = "sql/load.sql";

    public static void main(String[] args) throws IOException {
        configLogging();
        var properties = loadProperties();
        var preparedStatements = loadPreparedStatement();

        var url = properties.getProperty("jdbc.url");
        var user = properties.getProperty("jdbc.user");
        var password = properties.getProperty("jdbc.password");

        try (var connection = DriverManager.getConnection(url, user, password)) {

            connection.setAutoCommit(false);
            try (var statement = connection.createStatement()) {

                String createSql = getResourcesAsString(INIT_SQL_PATH);
                statement.execute(createSql);
                connection.commit();

                if (LOGGER.isLoggable(System.Logger.Level.INFO)) {
                    LOGGER.log(System.Logger.Level.INFO, "created the database tables with success");
                }

            }

            try (var statement = connection.createStatement()) {

                String data = getResourcesAsString(DATA_LOAD_PATH);
                var inserts = Stream.of(data.split(";"))
                        .collect(toList());
                for (var insert : inserts) {
                    statement.addBatch(insert);
                }
                int affected[] = statement.executeBatch();
                connection.commit();

                // log the inserted rows
                if (LOGGER.isLoggable(System.Logger.Level.INFO)) {

                    int total = 0;
                    for (int af : affected) {
                        total += af;
                    }
                    var message = String.format("the inserted rows is %d", total);
                    LOGGER.log(System.Logger.Level.INFO, message);
                }
            }
            String getAll = preparedStatements.getProperty("Person.selectAll.statement");
            try (var statement = connection.prepareStatement(getAll)) {

                var result = statement.executeQuery();
                                
                var strategies = extractionStrategy(preparedStatements, "Person.selectAll.strategy");
                var format = preparedStatements.getProperty("Person.selectAll.format");

                boolean hasNext = result.next();
                while (hasNext) {
                    System.out.printf(format, getData(result, strategies));
                    hasNext = result.next();
                }
            }

        } catch (Exception e) {
            LOGGER.log(System.Logger.Level.ERROR, "an unrecoverable exception!", e);

        }

    }

    private static Properties loadProperties() throws IOException {
        var properties = new Properties();
        try (var stream = loadStream(PROPERTIES_PATH)) {
            properties.load(stream);
        }
        return properties;
    }

    private static Properties loadPreparedStatement() throws IOException {
        var properties = new Properties();
        try (var stream = loadStream(PREPARED_STATEMENT)) {
            properties.load(stream);
        }
        return properties;
    }

    private static String getResourcesAsString(String path) throws IOException {

        try (
                var stream = loadStream(path);
                var reader = new InputStreamReader(stream, UTF_8);
                var buffer = new BufferedReader(reader)) {
            String result = buffer.lines().collect(Collectors.joining());
            return result;
        }
    }

    private static void configLogging() throws IOException {
        try (var logConfigStream = loadStream(LOGGING_PATH)) {
            LogManager.getLogManager().readConfiguration(logConfigStream);
        }
    }

    private static InputStream loadStream(String path) throws IOException {
        var stream = App.class.getClassLoader().getResourceAsStream(path);

        if (null == stream) {
            var message = String.format("could not load the resource file: %s", path);
            throw new IOException(message);
        }

        return stream;
    }

    private static List<ColumnStrategy> extractionStrategy(Properties properties, String property) {
        var strategy = properties.getProperty(property);
        return Stream.of(strategy.split(","))
                .map(ExtractionStrategy::valueOf)
                .collect(toList());
    }

    static interface ColumnStrategy {

        Object getValue(ResultSet result, int postion);
    }

    static enum ExtractionStrategy implements ColumnStrategy {
        INT {
            @Override
            public Integer getValue(ResultSet result, int position) {
                try {
                    return result.getInt(position);
                } catch (SQLException e) {
                    throw new RuntimeException("column extraction exception", e);
                }
            }
        },
        STRING {
            @Override
            public String getValue(ResultSet result, int position) {
                try {
                    return result.getString(position);
                } catch (SQLException e) {
                    throw new RuntimeException("column extraction exception", e);
                }
            }
        };

        @Override
        public Object getValue(ResultSet result, int postion) {
            throw new UnsupportedOperationException("this operation is unsupported");
        }
    }

    private static Object[] getData(ResultSet result, List<? extends ColumnStrategy> strategies) throws SQLException {
        return IntStream.rangeClosed(1, strategies.size())
                .mapToObj(i -> App.getColumnValue(result, i, strategies.get(i - 1)))
                .toArray(Object[]::new);
    }

    private static Object getColumnValue(ResultSet result, int position, ColumnStrategy strategy) {
        return strategy.getValue(result, position);
    }
}
