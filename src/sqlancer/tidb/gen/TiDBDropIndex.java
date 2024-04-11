package sqlancer.tidb.gen;

import sqlancer.IgnoreMeException;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.tidb.TiDBProvider;
import sqlancer.tidb.TiDBSchema;

public final class TiDBDropIndex {

    private TiDBDropIndex() {
    }

    public static SQLQueryAdapter generate(TiDBProvider.TiDBGlobalState globalState) {
        TiDBSchema.TiDBTable table = globalState.getSchema().getRandomTable();
        if (!table.hasIndexes()) {
            throw new IgnoreMeException();
        }
        StringBuilder sb = new StringBuilder();
        sb.append("DROP INDEX ");
        sb.append(table.getRandomIndex().getIndexName());
        sb.append(" ON ");
        sb.append(table.getName());
        return new SQLQueryAdapter(sb.toString(), new ExpectedErrors());
    }

}
