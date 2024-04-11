package sqlancer.tidb.gen;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.tidb.TiDBErrors;
import sqlancer.tidb.TiDBExpressionGenerator;
import sqlancer.tidb.TiDBProvider.TiDBGlobalState;
import sqlancer.tidb.TiDBSchema;
import sqlancer.tidb.ast.TiDBColumnReference;
import sqlancer.tidb.ast.TiDBExpression;
import sqlancer.tidb.ast.TiDBJoin;
import sqlancer.tidb.ast.TiDBSelect;
import sqlancer.tidb.ast.TiDBTableReference;
import sqlancer.tidb.visitor.TiDBVisitor;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public final class TiDBSelectGenerator {

    public static SQLQueryAdapter getQuery(TiDBGlobalState globalState) throws SQLException {
        TiDBSelect selectStatement = new TiDBSelect();
        TiDBSchema.TiDBTables selectTables = globalState.getSchema().getRandomTableNonEmptyTables();
        // TODO: choose subset of table columns
        List<TiDBExpression> fetchColumns = Arrays.asList(new TiDBColumnReference(selectTables.getColumns().get(0)));
        selectStatement.setFetchColumns(fetchColumns);
        TiDBExpressionGenerator gen = new TiDBExpressionGenerator(globalState).setColumns(selectTables.getColumns());
        List<TiDBSchema.TiDBTable> tables = selectTables.getTables();
        if (Randomly.getBoolean()) {
            TiDBHintGenerator.generateHints(selectStatement, tables);
        }
        List<TiDBExpression> tableList = tables.stream().map(TiDBTableReference::new).collect(Collectors.toList());
        List<TiDBExpression> joins = TiDBJoin.getJoins(tableList, globalState);
        // TODO: randomly generate join operation
        selectStatement.setJoinList(joins);
        selectStatement.setFromList(tableList);
        selectStatement.setWhereClause(null);
        if (Randomly.getBooleanWithRatherLowProbability()) {
            selectStatement.setOrderByExpressions(gen.generateOrderBys());
        }
        if (Randomly.getBoolean()) {
            selectStatement.setWhereClause(gen.generatePredicate());
        }
        if (Randomly.getBooleanWithRatherLowProbability()) {
            selectStatement.setGroupByExpressions(gen.generateExpressions(Randomly.smallNumber() + 1));
        }
        String queryString = TiDBVisitor.asString(selectStatement);
        
        ExpectedErrors errors = new ExpectedErrors();
        TiDBErrors.addExpressionErrors(errors);
        TiDBErrors.addExpressionHavingErrors(errors);
        return new SQLQueryAdapter(queryString, errors);
    }
}
