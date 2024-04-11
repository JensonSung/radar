package sqlancer.tidb.gen.transaction;

import java.sql.SQLException;

import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.tidb.TiDBProvider.TiDBGlobalState;

public class TiDBCommitGenerator {

    public static SQLQueryAdapter getQuery(TiDBGlobalState globalState) throws SQLException {
        return new SQLQueryAdapter("COMMIT");
    }
}
