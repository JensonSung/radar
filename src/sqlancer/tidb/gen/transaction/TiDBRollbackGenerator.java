package sqlancer.tidb.gen.transaction;

import java.sql.SQLException;

import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.tidb.TiDBProvider.TiDBGlobalState;

public class TiDBRollbackGenerator {

    public static SQLQueryAdapter getQuery(TiDBGlobalState globalState) throws SQLException {
        return new SQLQueryAdapter("ROLLBACK");
    }
}
