package sqlancer.cockroachdb.gen;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.cockroachdb.CockroachDBProvider;
import sqlancer.cockroachdb.CockroachDBSchema;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;

public final class CockroachDBDropIndex {

    private CockroachDBDropIndex() {
    }

    public static SQLQueryAdapter generate(CockroachDBProvider.CockroachDBGlobalState globalState) {
        CockroachDBSchema.CockroachDBTable table = Randomly.fromList(globalState.getSchema().getDatabaseTablesWithoutViews());
        if (!table.hasIndexes()) {
            throw new IgnoreMeException();
        }
        // https://www.cockroachlabs.com/docs/stable/drop-index.html
        StringBuilder sb = new StringBuilder(); // DROP INDEX indexName;
        sb.append("DROP INDEX ");
        sb.append(table.getRandomIndex().getIndexName());
        if (Randomly.getBoolean()) {
            sb.append(" CASCADE");
        }
        ExpectedErrors errors = new ExpectedErrors();
        errors.add("use as unique constraint");
        return new SQLQueryAdapter(sb.toString(), errors);
    }

}
