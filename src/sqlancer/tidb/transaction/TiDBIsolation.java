package sqlancer.tidb.transaction;

public class TiDBIsolation {

    public enum TiDBIsolationLevel {

        READ_COMMITTED("READ COMMITTED", "RC"),
        REPEATABLE_READ("REPEATABLE READ", "RR");

        private final String name;
        private final String alias; // TODO: as input to reproduce the test case

        TiDBIsolationLevel(String name, String alias) {
            this.name = name;
            this.alias = alias;
        }

        public String getName() {
            return name;
        }

        public String getAlias() {
            return alias;
        }
    }

    public static TiDBIsolationLevel getFromAlias(String alias) {
        for (TiDBIsolationLevel level : TiDBIsolationLevel.values()) {
            if (level.alias.equals(alias)) {
                return level;
            }
        }
        throw new RuntimeException("Invalid isolation level alias: " + alias);
    }
}
