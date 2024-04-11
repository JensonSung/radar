package sqlancer.cockroachdb;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import sqlancer.DBMSSpecificOptions;
import sqlancer.OracleFactory;
import sqlancer.cockroachdb.CockroachDBOptions.CockroachDBOracleFactory;
import sqlancer.cockroachdb.CockroachDBProvider.CockroachDBGlobalState;
import sqlancer.cockroachdb.oracle.CockroachDBEDCOracle;
import sqlancer.common.oracle.TestOracle;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

@Parameters(separators = "=", commandDescription = "CockroachDB (default port: " + CockroachDBOptions.DEFAULT_PORT
        + " default host: " + CockroachDBOptions.DEFAULT_HOST + ")")
public class CockroachDBOptions implements DBMSSpecificOptions<CockroachDBOracleFactory> {
    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 26257;

    @Parameter(names = "--oracle")
    public CockroachDBOracleFactory oracle = CockroachDBOracleFactory.EDC;

    public enum CockroachDBOracleFactory implements OracleFactory<CockroachDBGlobalState> {
        EDC {
            @Override
            public TestOracle create(CockroachDBGlobalState globalState) throws SQLException {
                return new CockroachDBEDCOracle(globalState);
            }

            @Override
            public boolean requiresAllTablesToContainRows() {
                return true;
            }
        }

    }

    @Parameter(names = {
            "--test-hash-indexes" }, description = "Test the USING HASH WITH BUCKET_COUNT=n_buckets option in CREATE INDEX")
    public boolean testHashIndexes = true;

    @Parameter(names = { "--test-temp-tables" }, description = "Test TEMPORARY tables")
    public boolean testTempTables; // default: false https://github.com/cockroachdb/cockroach/issues/85388

    @Parameter(names = {
            "--increased-vectorization" }, description = "Generate VECTORIZE=on with a higher probability (which found a number of bugs in the past)")
    public boolean makeVectorizationMoreLikely = true;

    @Override
    public List<CockroachDBOracleFactory> getTestOracleFactory() {
        return Arrays.asList(oracle);
    }

}
