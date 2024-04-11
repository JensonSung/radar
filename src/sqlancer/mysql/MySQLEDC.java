package sqlancer.mysql;

import sqlancer.SQLConnection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

public class MySQLEDC {

    public final MySQLGlobalState state;
    public final Map<String, String> createTableStatements = new HashMap<>(); // used to create equivalent database from rawDB

    public MySQLEDC(MySQLGlobalState state) {
        this.state = state;
    }


    public MySQLGlobalState createRawDB() throws SQLException {
        state.getState().logStatement("========Create RawDB========"); // do not need to log statements

        // build connection
        String url = String.format("jdbc:mysql://%s:%d?serverTimezone=UTC&useSSL=false&allowPublicKeyRetrieval=true",
                state.getOptions().getHost(), state.getOptions().getPort());
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
        for (MySQLSchema.MySQLTable table : state.getSchema().getDatabaseTablesWithoutViews()) {
            // get create table string
            StringBuilder createTableBuilder = new StringBuilder();
            createTableBuilder.append("CREATE TABLE ");
            createTableBuilder.append(table.getName());
            createTableBuilder.append("(");
            ResultSet resultSet = statement.executeQuery("SHOW FULL COLUMNS FROM " + state.getDatabaseName() + "." + table.getName());
            while (resultSet.next()) {
                createTableBuilder.append(resultSet.getString("Field")); //column name
                createTableBuilder.append(" ").append(resultSet.getString("Type")); //column type
                String collation = resultSet.getString("Collation");
                if (collation != null && !collation.equals("utf8mb4_0900_ai_ci")) { // do not need default collation
                    createTableBuilder.append(" ").append("COLLATE").append(" ").append("\"").append(collation).append("\"");
                }

                createTableBuilder.append(",");
            }
            String createTableString = createTableBuilder.toString();
            createTableString = createTableString.substring(0, createTableString.length() - 1); // remove the last comma
            createTableString += ")";

            // copy data
            state.getState().logStatement(createTableString);
            statement.execute(createTableString);
            String copyData = String.format("INSERT INTO %s SELECT * FROM %s", table.getName(), state.getDatabaseName() + "." + table.getName());
            state.getState().logStatement(copyData);
            statement.execute(copyData);

            // record create table statements for further equivalent database construction
            createTableStatements.put(table.getName(), createTableString);
        }

        // Currently, we do not create views in MySQL
        state.getState().logStatement("========Finish Create========");
        statement.close();

        MySQLGlobalState rawState = new MySQLGlobalState();
        rawState.setDatabaseName(rawDB);
        rawState.setConnection(new SQLConnection(conn));
        return rawState;
    }

}
