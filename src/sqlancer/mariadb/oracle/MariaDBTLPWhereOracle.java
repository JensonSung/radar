package sqlancer.mariadb.oracle;

import sqlancer.ComparatorHelper;
import sqlancer.Randomly;
import sqlancer.mariadb.MariaDBProvider;
import sqlancer.mariadb.ast.MariaDBStringVisitor;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MariaDBTLPWhereOracle extends MariaDBTLPBase{
    public MariaDBTLPWhereOracle(MariaDBProvider.MariaDBGlobalState state) {
        super(state);
    }
    @Override
    public void check() throws SQLException {
        super.check();
        select.setWhereClause(null);
        String originalQueryString = MariaDBStringVisitor.asString(select);
        List<String> resultSet = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors, state);
        if (Randomly.getBoolean()) {
            select.setOrderByExpressions(gen.generateOrderBys());
        }else {
            select.setOrderByExpressions(Collections.emptyList());
        }
        select.setWhereClause(predicate);
        String firstQueryString = MariaDBStringVisitor.asString(select);
        select.setWhereClause(negatedPredicate);
        String secondQueryString = MariaDBStringVisitor.asString(select);
        select.setWhereClause(isNullPredicate);
        String thirdQueryString = MariaDBStringVisitor.asString(select);
        List<String> combinedString = new ArrayList<>();
        List<String> secondResultSet = ComparatorHelper.getCombinedResultSet(firstQueryString, secondQueryString,
                thirdQueryString, combinedString, Randomly.getBoolean(), state, errors);
        ComparatorHelper.assumeResultSetsAreEqual(resultSet, secondResultSet, originalQueryString, combinedString,
                state);
    }
}
