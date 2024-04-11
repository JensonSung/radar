package sqlancer.mariadb;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import sqlancer.DBMSSpecificOptions;
import sqlancer.OracleFactory;
import sqlancer.common.oracle.TestOracle;
import sqlancer.mariadb.MariaDBOptions.MariaDBOracleFactory;
import sqlancer.mariadb.MariaDBProvider.MariaDBGlobalState;
import sqlancer.mariadb.oracle.MariaDBEDCOracle;
import sqlancer.mariadb.oracle.MariaDBNoRECOracle;
import sqlancer.mariadb.oracle.MariaDBTLPWhereOracle;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

@Parameters(separators = "=", commandDescription = "MariaDB (default port: " + MariaDBOptions.DEFAULT_PORT
        + ", default host: " + MariaDBOptions.DEFAULT_HOST + ")")
public class MariaDBOptions implements DBMSSpecificOptions<MariaDBOracleFactory> {
    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 3306;

    @Parameter(names = "--oracle")
    public List<MariaDBOracleFactory> oracles = Arrays.asList(MariaDBOracleFactory.NOREC);

    public enum MariaDBOracleFactory implements OracleFactory<MariaDBGlobalState> {

        NOREC {

            @Override
            public TestOracle<MariaDBGlobalState> create(MariaDBGlobalState globalState) throws SQLException {
                return new MariaDBNoRECOracle(globalState);
            }

        },
        TLP_WHERE {

            @Override
            public TestOracle<MariaDBGlobalState> create(MariaDBGlobalState globalState) throws SQLException {
                return new MariaDBTLPWhereOracle(globalState);
            }
        },
        EDC {

            @Override
            public TestOracle<MariaDBGlobalState> create(MariaDBGlobalState globalState) throws SQLException {
                return new MariaDBEDCOracle(globalState);
            }

        }
    }

    @Override
    public List<MariaDBOracleFactory> getTestOracleFactory() {
        return oracles;
    }

}
