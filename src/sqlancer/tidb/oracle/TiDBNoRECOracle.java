package sqlancer.tidb.oracle;

import sqlancer.IgnoreMeException;
import sqlancer.common.oracle.NoRECBase;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.tidb.TiDBErrors;
import sqlancer.tidb.TiDBExpressionGenerator;
import sqlancer.tidb.TiDBProvider.TiDBGlobalState;
import sqlancer.tidb.TiDBSchema;
import sqlancer.tidb.TiDBSchema.TiDBTables;
import sqlancer.tidb.TiDBSchema.TiDBColumn;
import sqlancer.tidb.ast.TiDBColumnReference;
import sqlancer.tidb.ast.TiDBExpression;
import sqlancer.tidb.ast.TiDBPostfixText;
import sqlancer.tidb.ast.TiDBSelect;
import sqlancer.tidb.ast.TiDBTableReference;
import sqlancer.tidb.ast.TiDBUnaryPostfixOperation;
import sqlancer.tidb.visitor.TiDBVisitor;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class TiDBNoRECOracle extends NoRECBase<TiDBGlobalState> implements TestOracle<TiDBGlobalState> {

    private final TiDBSchema schema;

    private static final int EMPTY = -1;

    public TiDBNoRECOracle(TiDBGlobalState state) {
        super(state);
        this.schema = state.getSchema();
        TiDBErrors.addExpressionErrors(errors);
        errors.add("Empty pattern is invalid in regexp");
        errors.add("Invalid regexp pattern");
    }

    @Override
    public void check() throws SQLException {
        TiDBTables randomTables = schema.getRandomTableNonEmptyTables();
        List<TiDBColumn> columns = randomTables.getColumns();
        TiDBExpressionGenerator expressionGenerator = new TiDBExpressionGenerator(state).setColumns(columns);
        TiDBExpression randomWhereCondition = expressionGenerator.generateExpression();
        List<TiDBExpression> tableRefs = randomTables.getTables().stream()
                .map(TiDBTableReference::new).collect(Collectors.toList());
        TiDBSelect select = new TiDBSelect();
        select.setFromList(tableRefs);
        int optimizedResult = getOptimizedResult(select, randomWhereCondition);
        int nonOptimizedResult = getNonOptimizedResult(select, randomWhereCondition);
        if (optimizedResult == EMPTY || nonOptimizedResult == EMPTY)
            throw new IgnoreMeException();
        if (optimizedResult != nonOptimizedResult) {
            state.getState().getLocalState().log(optimizedQueryString + ";\n" + unoptimizedQueryString + ";");
            throw new AssertionError(String.format("Optimized count: %d does not match NonOptimized count: %d",
                    optimizedResult, nonOptimizedResult));
        }

    }


    private int getOptimizedResult(TiDBSelect select, TiDBExpression randomWhereCondition) {
        select.setWhereClause(randomWhereCondition);
        TiDBColumnReference allColumns = new TiDBColumnReference(new TiDBColumn("COUNT(*)", new TiDBSchema.TiDBCompositeDataType(TiDBSchema.TiDBDataType.INT), false, false));
        select.setFetchColumns(Arrays.asList(allColumns));
        optimizedQueryString = TiDBVisitor.asString(select);
        return executeQuery(optimizedQueryString);
    }

    private int getNonOptimizedResult(TiDBSelect select, TiDBExpression randomWhereCondition) {
        TiDBUnaryPostfixOperation isTrue = new TiDBUnaryPostfixOperation(randomWhereCondition, TiDBUnaryPostfixOperation.TiDBUnaryPostfixOperator.IS_TRUE);
        TiDBPostfixText tiDBPostfixText = new TiDBPostfixText(isTrue, " as count", false);
        select.setFetchColumns(Arrays.asList(tiDBPostfixText));
        select.setWhereClause(null);
        unoptimizedQueryString = "SELECT SUM(count) FROM (" + TiDBVisitor.asString(select) + ") as selectedRefs ";
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
