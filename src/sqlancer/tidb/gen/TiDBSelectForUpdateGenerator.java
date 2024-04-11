package sqlancer.tidb.gen;

import java.sql.SQLException;

import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.tidb.TiDBProvider.TiDBGlobalState;

public final class TiDBSelectForUpdateGenerator {

    public static SQLQueryAdapter getQuery(TiDBGlobalState globalState) throws SQLException {
        SQLQueryAdapter selectStatement = TiDBSelectGenerator.getQuery(globalState);
        String selectForUpdateStatement = selectStatement.getQueryString().replace(";", "") + " FOR UPDATE;";
        return new SQLQueryAdapter(selectForUpdateStatement, selectStatement.getExpectedErrors());
    }
}
