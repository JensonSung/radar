package sqlancer.mysql.oracle;

import sqlancer.IgnoreMeException;
import sqlancer.common.oracle.NoRECBase;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.mysql.MySQLGlobalState;
import sqlancer.mysql.MySQLSchema;
import sqlancer.mysql.MySQLVisitor;
import sqlancer.mysql.ast.*;
import sqlancer.mysql.gen.MySQLExpressionGenerator;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class MySQLNoRECOracle extends NoRECBase<MySQLGlobalState> implements TestOracle<MySQLGlobalState> {
    private final MySQLSchema schema;
    private static final int EMPTY = -1;

    public MySQLNoRECOracle(MySQLGlobalState state) {
        super(state);
        this.schema = state.getSchema();
    }

    @Override
    public void check() throws SQLException {
        MySQLSchema.MySQLTables randomTables = schema.getRandomTableNonEmptyTables();
        List<MySQLSchema.MySQLColumn> columns = randomTables.getColumns();
        MySQLExpressionGenerator generator = new MySQLExpressionGenerator(state).setColumns(columns);
        MySQLExpression randomWhereCondition = generator.generateExpression();
        List<MySQLExpression> tableRefs = randomTables.getTables().stream()
                .map(MySQLTableReference::new).collect(Collectors.toList());
        MySQLSelect select = new MySQLSelect();
        select.setFromList(tableRefs);
        select.setSelectType(MySQLSelect.SelectType.ALL);
        int optimizedResult = getOptimizedResult(select, randomWhereCondition);
        int nonOptimizedResult = getNonOptimizedResult(select, randomWhereCondition);
        if(optimizedResult == EMPTY || nonOptimizedResult == EMPTY)
            throw new  IgnoreMeException();
        if (optimizedResult != nonOptimizedResult) {
            state.getState().getLocalState().log(optimizedQueryString + ";\n" + unoptimizedQueryString + ";");
            throw new AssertionError(String.format("Optimized count: %d does not match NonOptimized count: %d",
                    optimizedResult,nonOptimizedResult));
        }
    }

    private int getOptimizedResult(MySQLSelect select, MySQLExpression randomWhereCondition) {
        select.setWhereClause(randomWhereCondition);
//        MySQLSelectAll column = new MySQLSelectAll();
//        select.setFetchColumns(Arrays.asList(column));
        MySQLColumnReference allColumns = new MySQLColumnReference(new MySQLSchema.MySQLColumn("*", MySQLSchema.MySQLDataType.INT,
        false,0),null);
//        allColumns.setHasQuotes(false);
        select.setFetchColumns(Arrays.asList(allColumns));
        optimizedQueryString = MySQLVisitor.asString(select);
        return executeQuery(optimizedQueryString);
    }

    private int getNonOptimizedResult(MySQLSelect select, MySQLExpression randomWhereCondition) {
        MySQLUnaryPostfixOperation isTrue = new MySQLUnaryPostfixOperation(randomWhereCondition, MySQLUnaryPostfixOperation.UnaryPostfixOperator.IS_TRUE, false);
        MySQLPostfixText mySQLPostfixText = new MySQLPostfixText(isTrue, " as count", false);
        select.setFetchColumns(Arrays.asList(mySQLPostfixText));
        select.setWhereClause(null);
        unoptimizedQueryString = "SELECT SUM(count) FROM (" + MySQLVisitor.asString(select) + ") as selectedRefs ";
        return executeQuery(unoptimizedQueryString);
    }

    private int executeQuery(String query) {
        int count = 0;
        SQLQueryAdapter queryAdapter = new SQLQueryAdapter(query, errors);
        try (SQLancerResultSet rs = queryAdapter.executeAndGet(state)) {
            if (rs == null)
                return EMPTY;
            else
                try {
                    while (rs.next()) {
                        count += rs.getInt(1);
                    }
                } catch (SQLException e) {
                    count = EMPTY;
                }
        } catch (Exception e) {
            if (e instanceof IgnoreMeException) {
                throw (IgnoreMeException) e;
            }
            throw new AssertionError(query, e);
        }
        return count;
    }
}
