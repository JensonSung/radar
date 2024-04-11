package sqlancer.cockroachdb;

import sqlancer.SQLConnection;
import sqlancer.common.query.Query;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class CockroachDBEDC {

    public final CockroachDBProvider.CockroachDBGlobalState state;
    public final List<Query<?>> knownToReproduce; // record to original statements
    public final List<Query<?>> validStatements = new ArrayList<>(); // used to construct oriDB

    public CockroachDBEDC(CockroachDBProvider.CockroachDBGlobalState state) {
        this.state = state;
        this.knownToReproduce = new ArrayList<>(state.getState().getStatements());
    }


    public CockroachDBProvider.CockroachDBGlobalState createRawDB() throws SQLException {
        state.getState().logStatement("========Create RawDB========");

        // build connection
        String url = String.format("jdbc:postgresql://%s:%d/%s", state.getOptions().getHost(), state.getOptions().getPort(), state.getDatabaseName());
        Connection conn = DriverManager.getConnection(url, state.getOptions().getUserName(), state.getOptions().getPassword());
        Statement statement = conn.createStatement();

        // create rawDB
        String rawDB = state.getDatabaseName() + "_raw";
        state.getState().logStatement("DROP DATABASE IF EXISTS " + rawDB + " CASCADE");
        state.getState().logStatement("CREATE DATABASE " + rawDB);
        statement.execute("DROP DATABASE IF EXISTS " + rawDB);
        statement.execute("CREATE DATABASE " + rawDB);

        // connect rawDB
        state.getState().logStatement("USE " + rawDB);
        statement.execute("USE " + rawDB);

        for (CockroachDBSchema.CockroachDBTable table : state.getSchema().getDatabaseTablesWithoutViews()) {
            // get create table string
            StringBuilder createTable = new StringBuilder();
            createTable.append("CREATE TABLE ");
            createTable.append(table.getName());
            createTable.append("(");
            ResultSet resultSet = statement.executeQuery("SHOW COLUMNS FROM " + state.getDatabaseName() + ".public." + table.getName());
            while (resultSet.next()) {
                boolean isHidden = resultSet.getBoolean("is_hidden");
                if (isHidden) {
                    continue;
                }
                String columnName = resultSet.getString("column_name");
                createTable.append(columnName); //column name
                createTable.append(" ").append(resultSet.getString("data_type")); //column type, which contains collation for string type
                createTable.append(",");
            }
            String createTableString = createTable.toString();
            createTableString = createTableString.substring(0, createTableString.length() - 1); // remove the last comma
            createTableString += ")";

            //create equivalent table
            state.getState().logStatement(createTableString);
            statement.execute(createTableString);
            String equTable = String.format("INSERT INTO %s SELECT * FROM %s", table.getName(), state.getDatabaseName() + ".public." + table.getName());
            state.getState().logStatement(equTable);
            statement.execute(equTable);
        }

        //copy all views
        assert validStatements.isEmpty();
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

        CockroachDBProvider.CockroachDBGlobalState rawState = new CockroachDBProvider.CockroachDBGlobalState();
        rawState.setConnection(new SQLConnection(conn));
        rawState.setDatabaseName(rawDB);
        return rawState;
    }

}
