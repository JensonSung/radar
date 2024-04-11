package sqlancer.tidb;

import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.common.query.Query;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.tidb.gen.TiDBDropIndex;
import sqlancer.tidb.gen.TiDBIndexGenerator;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TiDBEDC {

    public final TiDBProvider.TiDBGlobalState state;
    public final List<Query<?>> knownToReproduce; // record to original statements
    public final List<Query<?>> validStatements = new ArrayList<>(); // used to construct oriDB, filer out invalid statements
    public final List<Query<?>> mutationStatements = new ArrayList<>(); // used to mutate metadata, especially for data distributions, and configurations.
    public final Map<String, Map<String, List<String>>> constraintMapTableColumns = new HashMap<>(); // used to record all data constraints


    public TiDBEDC(TiDBProvider.TiDBGlobalState state) {
        this.state = state;
        this.knownToReproduce = new ArrayList<>(state.getState().getStatements());
    }

    public TiDBProvider.TiDBGlobalState createRawDB() throws SQLException {
        state.getState().logStatement("========Create RawDB========");

        // build connection
        String url = String.format("jdbc:mysql://%s:%d/", state.getOptions().getHost(), state.getOptions().getPort());
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

        //copy all tables without view
        for (TiDBSchema.TiDBTable table : state.getSchema().getDatabaseTablesWithoutViews()) {
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
            String equTable = String.format("INSERT INTO %s SELECT * FROM %s", table.getName(), state.getDatabaseName() + "." + table.getName());
            state.getState().logStatement(equTable);
            statement.execute(equTable);
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

        TiDBProvider.TiDBGlobalState rawState = new TiDBProvider.TiDBGlobalState();
        rawState.setConnection(new SQLConnection(conn));
        rawState.setDatabaseName(rawDB);
        return rawState;
    }

    public void recordConstraint(String constraintType, String tableName, String expression) {
        if (!constraintMapTableColumns.containsKey(constraintType)) {
            constraintMapTableColumns.put(constraintType, new HashMap<>());
        }
        if (!constraintMapTableColumns.get(constraintType).containsKey(tableName)) {
            constraintMapTableColumns.get(constraintType).put(tableName, new ArrayList<>());
        }
        constraintMapTableColumns.get(constraintType).get(tableName).add(expression);
    }

    public String getRandomConstraintName(Statement statement, String schemaName, String tableName, String constraintType) throws SQLException {
        String checkConstraintName = String.format("SELECT CONSTRAINT_NAME FROM information_schema.TABLE_CONSTRAINTS WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s' AND CONSTRAINT_TYPE='%s'", schemaName, tableName, constraintType);
        ResultSet resultSet = statement.executeQuery(checkConstraintName);
        List<String> candidateNames = new ArrayList<>();
        while (resultSet.next()) {
            candidateNames.add(resultSet.getString("CONSTRAINT_NAME"));
        }

        return Randomly.fromList(candidateNames);
    }

    public TiDBProvider.TiDBGlobalState createEquDBFromOriDB(int id) throws SQLException {
        state.getState().logStatement("========Create EquDB_" + id + " From OriDB ========");

        // build connection
        String url = String.format("jdbc:mysql://%s:%d?serverTimezone=UTC&useSSL=false&allowPublicKeyRetrieval=true",
                state.getOptions().getHost(), state.getOptions().getPort());
        Connection conn = DriverManager.getConnection(url, state.getOptions().getUserName(), state.getOptions().getPassword());
        Statement statement = conn.createStatement();

        // create equDB
        String equDB = state.getDatabaseName() + "_equ_" + id;
        state.getState().logStatement("DROP DATABASE IF EXISTS " + equDB);
        state.getState().logStatement("CREATE DATABASE " + equDB);
        statement.execute("DROP DATABASE IF EXISTS " + equDB);
        statement.execute("CREATE DATABASE " + equDB);

        // connect equDB
        state.getState().logStatement("USE " + equDB);
        statement.execute("USE " + equDB);

        if (validStatements.isEmpty()) {
            // record valid statements for further equivalent database construction
            for (Query<?> query : knownToReproduce) {
                try {
                    statement.execute(query.getQueryString());
                    state.getState().logStatement(query.getQueryString());
                    if (query.getQueryString().startsWith("SET")) {
                        mutationStatements.add(query); // record statements related to configurations
                    } else if (query.getQueryString().startsWith("ANALYZE")) {
                        mutationStatements.add(query); // record statements related to collect metadata
                    } else {
                        validStatements.add(query);
                    }
                } catch (SQLException ignored) {
                }
            }

            // get available data constraints
            for (TiDBSchema.TiDBTable table : state.getSchema().getDatabaseTablesWithoutViews()) {
                String getTableStructure = String.format("SHOW CREATE TABLE %s", table.getName());
                ResultSet resultSet = statement.executeQuery(getTableStructure);
                if (resultSet.next()) {
                    String[] expressions = resultSet.getString("Create Table").split("\n");
                    for (int i = 0; i < expressions.length - 1; i++) {
                        String expression = expressions[i];
                        expression = expression.substring(0, expression.length() - 1); // remove comma
                        if (expression.contains("NOT NULL")) {
                            recordConstraint("NOT NULL", table.getName(), expression);
                        } else if (expression.contains("DEFAULT")) {
                            recordConstraint("DEFAULT", table.getName(), expression);
                        } else if (expression.contains("GENERATED")) {
                            recordConstraint("GENERATED", table.getName(), expression);
                        } else if (expression.contains("PRIMARY KEY")) {
                            recordConstraint("PRIMARY KEY", table.getName(), expression);
                        } else if (expression.contains("CHECK")) {
                            recordConstraint("CHECK", table.getName(), expression);
                        } else if (expression.contains("UNIQUE KEY")) {
                            recordConstraint("UNIQUE KEY", table.getName(), expression);
                        } else if (expression.contains("FOREIGN KEY")) {
                            recordConstraint("FOREIGN KEY", table.getName(), expression);
                        }
                    }
                }
            }

        } else {
            for (Query<?> query : validStatements) {
                try {
                    statement.execute(query.getQueryString());
                    state.getState().logStatement(query.getQueryString());
                } catch (SQLException ignored) {
                }
            }
        }

        // record mutations
        List<Query<?>> mutationStatements = new ArrayList<>();

        // mutate data constraints
        List<String> removeConstraints;
        if (constraintMapTableColumns.keySet().isEmpty()) {
            removeConstraints = new ArrayList<>();
        } else {
            removeConstraints = Randomly.nonEmptySubset(new ArrayList<>(constraintMapTableColumns.keySet()));
        }
        for (String constraint : removeConstraints) {
            String tableName = Randomly.fromList(new ArrayList<>(constraintMapTableColumns.get(constraint).keySet()));
            String expression = Randomly.fromList(constraintMapTableColumns.get(constraint).get(tableName));
            switch (constraint) {
                case "NOT NULL":
                case "DEFAULT":
                case "GENERATED": {
                    expression = expression.substring(0, expression.indexOf(constraint));
                    String alterTable = String.format("ALTER TABLE %s MODIFY COLUMN %s", tableName, expression);
                    mutationStatements.add(new SQLQueryAdapter(alterTable));
                    break;
                }
                case "PRIMARY KEY": {
                    String alterTable = String.format("ALTER TABLE %s DROP PRIMARY KEY", tableName);
                    mutationStatements.add(new SQLQueryAdapter(alterTable));
                    break;
                }
                case "CHECK":
                case "FOREIGN KEY": {
                    String constraintName = getRandomConstraintName(statement, equDB, tableName, constraint);
                    String alterTable = String.format("ALTER TABLE %s DROP CONSTRAINT %s", tableName, constraintName);
                    mutationStatements.add(new SQLQueryAdapter(alterTable));
                    break;
                }
                case "UNIQUE KEY": {
                    // TiDB treats UNIQUE as indexes, we ignore it here
                    break;
                }
                default:
                    throw new RuntimeException("unknown constraint: " + constraint);
            }
        }

        // mutate indexes
        for (TiDBSchema.TiDBTable table : state.getSchema().getDatabaseTablesWithoutViews()) {
            if (table.getIndexes().isEmpty()) {
                if (Randomly.getBooleanWithRatherLowProbability()) {
                    mutationStatements.add(TiDBIndexGenerator.getQuery(state));
                }
            } else if (table.hasPrimaryKey() && table.getIndexes().size() == 1) {
                // we do not mutate PRIMARY KEY in indexes
            } else {
                if (Randomly.getBoolean()) {
                    mutationStatements.add(TiDBDropIndex.generate(state));
                }
            }
        }

        // we do not mutate data distributions, because MySQL does not support VACUUM statements

        // mutate configurations
        if (!mutationStatements.isEmpty()) {
            mutationStatements.addAll(Randomly.nonEmptySubset(mutationStatements));
        }

        // apply mutations
        for (Query<?> mutation : mutationStatements) {
            try {
                statement.execute(mutation.getQueryString());
                state.getState().logStatement(mutation.getQueryString());
            } catch (SQLException ignored) {
            }
        }

        state.getState().logStatement("========Finish Create========");
        statement.close();

        TiDBProvider.TiDBGlobalState equState = new TiDBProvider.TiDBGlobalState();
        equState.setDatabaseName(equDB);
        equState.setConnection(new SQLConnection(conn));
        return equState;
    }


}
