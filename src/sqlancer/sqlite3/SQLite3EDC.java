package sqlancer.sqlite3;

import sqlancer.SQLConnection;
import sqlancer.common.query.Query;
import sqlancer.sqlite3.schema.SQLite3Schema;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class SQLite3EDC {

    public final SQLite3GlobalState state;
    public final List<Query<?>> knownToReproduce; // record to original statements


    public SQLite3EDC(SQLite3GlobalState state) {
        this.state = state;
        this.knownToReproduce = new ArrayList<>(state.getState().getStatements());
    }

    public SQLite3GlobalState createRawDB() throws SQLException {
        state.getState().logStatement("========Create RawDB========");

        // build connection, create rawDB and connect rawDB
        File dir = new File("." + File.separator + "databases");
        while (!dir.exists()) {
            dir.mkdir();
        }
        File dataBase = new File(dir, state.getDatabaseName() + "_raw.db");
        while (dataBase.exists()) {
            dataBase.delete();
        }
        String url = "jdbc:sqlite:" + dataBase.getAbsolutePath();
        Connection conn = DriverManager.getConnection(url);
        Statement statement = conn.createStatement();

        // copy data to rawDB
        // because SQLite does not support multiple database instances, we need to replay the database creating process and reset all configurations
        // replay the database creating process
        for (Query<?> query : knownToReproduce) {
            try {
                statement.execute(query.getQueryString());
                state.getState().logStatement(query.getQueryString()); // only record valid statement
            } catch (SQLException ignored) {
            }
        }

        // backup data
        for (SQLite3Schema.SQLite3Table table : state.getSchema().getDatabaseTablesWithoutViews()) {
            statement.execute("DROP TABLE IF EXISTS " + table.getName() + "_edc");
            String createEquTable = String.format("CREATE TABLE %s_edc AS SELECT * FROM %s", table.getName(), table.getName());
            state.getState().logStatement(createEquTable);
            statement.execute(createEquTable);
        }

        // drop views
        for (SQLite3Schema.SQLite3Table view : state.getSchema().getViews()) {
            String dropView = String.format("DROP VIEW IF EXISTS %s", view.getName());
            state.getState().logStatement(dropView);
            statement.execute(dropView);
        }

        for (SQLite3Schema.SQLite3Table table : state.getSchema().getDatabaseTablesWithoutViews()) {
            // get create table string
            StringBuilder createTable = new StringBuilder();
            createTable.append("CREATE TABLE ");
            createTable.append(table.getName());
            createTable.append("(");
            String getTableStructure = String.format("PRAGMA table_info(%s)", table.getName());
            ResultSet resultSet = statement.executeQuery(getTableStructure);
            while (resultSet.next()) {
                createTable.append(resultSet.getString("name")); // column name
                createTable.append(" ").append(resultSet.getString("type")); // column type
                // we cannot get collations from SQLite, that we do not generate them
                createTable.append(",");
            }
            String createTableString = createTable.toString();
            createTableString = createTableString.substring(0, createTableString.length() - 1); // remove the last comma
            createTableString += ")";

            // drop the original table
            String dropTable = String.format("DROP TABLE %s", table.getName());
            state.getState().logStatement(dropTable);
            statement.execute(dropTable);

            // copy data from the backup table
            state.getState().logStatement(createTableString);
            statement.execute(createTableString);
            String insertData = String.format("INSERT INTO %s SELECT * FROM %s_edc", table.getName(), table.getName());
            state.getState().logStatement(insertData);
            statement.execute(insertData);

            // drop the backup table
            String dropEquTable = String.format("DROP TABLE %s_edc", table.getName());
            state.getState().logStatement(dropEquTable);
            statement.execute(dropEquTable);
        }

        // copy all views
        for (Query<?> query : knownToReproduce) {
            String queryString = query.getQueryString();
            if (queryString.contains("VIEW")) {
                try {
                    statement.execute(queryString);
                    state.getState().logStatement(queryString);
                } catch (SQLException ignored) { // skip invalid statements
                }
            }
        }

        state.getState().logStatement("========Finish Create========");
        statement.close();

        SQLite3GlobalState rawState = new SQLite3GlobalState();
        rawState.setConnection(new SQLConnection(conn));
        rawState.setDatabaseName(state.getDatabaseName() + "_raw");
        return rawState;
    }

}
