package sqlancer.sqlite3.oracle;

import sqlancer.Randomly;
import sqlancer.common.oracle.EDCBase;
import sqlancer.common.oracle.TestOracle;
import sqlancer.sqlite3.SQLite3EDC;
import sqlancer.sqlite3.SQLite3Errors;
import sqlancer.sqlite3.SQLite3GlobalState;
import sqlancer.sqlite3.SQLite3Visitor;
import sqlancer.sqlite3.ast.SQLite3Expression;
import sqlancer.sqlite3.ast.SQLite3Select;
import sqlancer.sqlite3.gen.SQLite3Common;
import sqlancer.sqlite3.gen.SQLite3ExpressionGenerator;
import sqlancer.sqlite3.schema.SQLite3Schema;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SQLite3EDCOracle extends EDCBase<SQLite3GlobalState> implements TestOracle<SQLite3GlobalState> {

    public SQLite3EDCOracle(SQLite3GlobalState originalState) {
        super(originalState);
        SQLite3Errors.addExpectedExpressionErrors(errors);
        SQLite3Errors.addMatchQueryErrors(errors);
        SQLite3Errors.addQueryErrors(errors);
        errors.add("misuse of aggregate");
        errors.add("misuse of window function");
        errors.add("second argument to nth_value must be a positive integer");
        errors.add("no such table");
        errors.add("no query solution");
        errors.add("unable to use function MATCH in the requested context");
    }

    @Override
    public Map<String, Map<String, List<String>>> obtainTableSchemas(SQLite3GlobalState state) throws SQLException {
        Map<String, Map<String, List<String>>> tableSchema = new HashMap<>();
        for (SQLite3Schema.SQLite3Table table : state.getSchema().getDatabaseTablesWithoutViews()) {
            String tableName = table.getName();
            try (Statement statement = state.getConnection().createStatement()) {
                tableSchema.put(tableName, new HashMap<>());
                String getColumnInfo = String.format("PRAGMA TABLE_INFO(%s)", tableName);
                ResultSet resultSet = statement.executeQuery(getColumnInfo);
                while (resultSet.next()) {
                    List<String> metaElements = new ArrayList<>();
                    String dataType = resultSet.getString("type");
                    String columnName = resultSet.getString("name");
                    boolean isNullable = resultSet.getBoolean("notnull");
                    String hasDefault = resultSet.getString("dflt_value");
                    boolean isPK = resultSet.getBoolean("pk");
                    metaElements.add(dataType);
                    if (!isNullable) {
                        metaElements.add("NOT NULL");
                    }
                    if (hasDefault != null) {
                        metaElements.add("DEFAULT " + hasDefault);
                    }
                    if (isPK) {
                        metaElements.add("PRIMARY KEY");
                    }
                    tableSchema.get(tableName).put(columnName, metaElements);
                }
            }
        }
        return tableSchema;
    }

    @Override
    public SQLite3GlobalState constructEquivalentState(SQLite3GlobalState state) {
        try {
            SQLite3EDC edc = new SQLite3EDC(state);
            return edc.createRawDB();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String generateQueryString(SQLite3GlobalState state) {
        SQLite3Schema.SQLite3Tables randomTables = state.getSchema().getRandomTableNonEmptyTables();
        List<SQLite3Schema.SQLite3Column> columns = randomTables.getColumns();
        SQLite3ExpressionGenerator gen = new SQLite3ExpressionGenerator(state).setColumns(columns).deterministicOnly();
        SQLite3Expression randomWhereCondition = gen.generateExpression();
        List<SQLite3Schema.SQLite3Table> tables = randomTables.getTables();
        List<SQLite3Expression.Join> joinStatements = gen.getRandomJoinClauses(tables);
        List<SQLite3Expression> tableRefs = SQLite3Common.getTableRefs(tables, state.getSchema());
        SQLite3Select select = new SQLite3Select();
        select.setFetchColumns((Randomly.nonEmptySubset(randomTables.getColumns()).stream().map(c -> new SQLite3Expression.SQLite3ColumnName(c, null)).collect(Collectors.toList())));
        select.setFromTables(tableRefs);
        select.setJoinClauses(joinStatements);
        select.setWhereClause(randomWhereCondition);
        if (Randomly.getBoolean()) {
            select.setOrderByExpressions(gen.generateOrderBys());
        }

        String query = SQLite3Visitor.asString(select);
        query = query.replaceAll(" INDEXED BY i\\d+", ""); // remove index reference INDEXED BY i10
        return query;
    }

}
