package sqlancer.mariadb;

import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.common.query.Query;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.mariadb.gen.MariaDBDropIndex;
import sqlancer.mariadb.gen.MariaDBIndexGenerator;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MariaDBEDC {

    public final MariaDBProvider.MariaDBGlobalState state;
    public final List<Query<?>> knownToReproduce; // record to original statements

    public MariaDBEDC(MariaDBProvider.MariaDBGlobalState state) {
        this.state = state;
        this.knownToReproduce = new ArrayList<>(state.getState().getStatements());
    }

    public MariaDBProvider.MariaDBGlobalState createRawDB() throws SQLException {
        state.getState().logStatement("========Create EquDB========");

        // build connection
        String url = String.format("jdbc:mariadb://%s:%d", state.getOptions().getHost(), state.getOptions().getPort());
        Connection conn = DriverManager.getConnection(url, state.getOptions().getUserName(), state.getOptions().getPassword());
        Statement statement = conn.createStatement();

        // create rawDB
        String rawDB = state.getDatabaseName() + "_raw";
        state.getState().logStatement("DROP DATABASE IF EXISTS " + rawDB);
        state.getState().logStatement("CREATE DATABASE " + rawDB);
        statement.execute("DROP DATABASE IF EXISTS " + rawDB);
        statement.execute("CREATE DATABASE " + rawDB);

        // connect rawDB
        state.getState().logStatement("USE " + rawDB);
        statement.execute("USE " + rawDB);

        // copy data to rawDB
        for (MariaDBSchema.MariaDBTable table : state.getSchema().getDatabaseTablesWithoutViews()) {
            // get create table string
            StringBuilder createTable = new StringBuilder();
            createTable.append("CREATE TABLE ");
            createTable.append(table.getName());
            createTable.append("(");
            ResultSet resultSet = statement.executeQuery("SHOW FULL COLUMNS FROM " + state.getDatabaseName() + "." + table.getName());
            while (resultSet.next()) {
                createTable.append(resultSet.getString("Field")); //column name
                createTable.append(" ").append(resultSet.getString("Type")); //column type
                String collation = resultSet.getString("Collation");
                if (collation != null) {
                    createTable.append(" ").append("COLLATE").append(" ").append("\"").append(collation).append("\"");
                }
                createTable.append(",");
            }
            String createTableString = createTable.toString();
            createTableString = createTableString.substring(0, createTableString.length() - 1); // remove the last comma
            createTableString += ")";

            // copy data
            state.getState().logStatement(createTableString);
            statement.execute(createTableString);
            String copyData = String.format("INSERT INTO %s SELECT * FROM %s", table.getName(), state.getDatabaseName() + "." + table.getName());
            state.getState().logStatement(copyData);
            statement.execute(copyData);
        }

        state.getState().logStatement("========Finish Create========");
        statement.close();

        MariaDBProvider.MariaDBGlobalState rawState = new MariaDBProvider.MariaDBGlobalState();
        rawState.setDatabaseName(rawDB);
        rawState.setConnection(new SQLConnection(conn));
        return rawState;
    }

}
