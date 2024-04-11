package sqlancer.common.schema;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import sqlancer.GlobalState;
import sqlancer.IgnoreMeException;
import sqlancer.Randomly;

public class AbstractSchema<G extends GlobalState<?, ?, ?>, T extends AbstractTable<?, ?, G>> {

    private final List<T> databaseTables;

    public AbstractSchema(List<T> databaseTables) {
        this.databaseTables = Collections.unmodifiableList(databaseTables);
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        for (T t : getDatabaseTables()) {
            sb.append(t);
            sb.append("\n");
        }
        return sb.toString();
    }

    public T getRandomTable() {
        return Randomly.fromList(getDatabaseTables());
    }

    public T getRandomTableOrBailout() {
        if (databaseTables.isEmpty()) {
            throw new IgnoreMeException();
        } else {
            return Randomly.fromList(getDatabaseTables());
        }
    }

    public T getRandomTable(Predicate<T> predicate) {
        return Randomly.fromList(getDatabaseTables().stream().filter(predicate).collect(Collectors.toList()));
    }

    public T getRandomTableOrBailout(Function<T, Boolean> f) {
        List<T> relevantTables = databaseTables.stream().filter(t -> f.apply(t)).collect(Collectors.toList());
        if (relevantTables.isEmpty()) {
            throw new IgnoreMeException();
        }
        return Randomly.fromList(relevantTables);
    }

    public List<T> getDatabaseTables() {
        return databaseTables;
    }

    public List<T> getTables(Predicate<T> predicate) {
        return databaseTables.stream().filter(predicate).collect(Collectors.toList());
    }

    public List<T> getDatabaseTablesRandomSubsetNotEmpty() {
        return Randomly.nonEmptySubset(databaseTables);
    }

    public T getDatabaseTable(String name) {
        return databaseTables.stream().filter(t -> t.getName().equals(name)).findAny().orElse(null);
    }

    public List<T> getViews() {
        return databaseTables.stream().filter(t -> t.isView()).collect(Collectors.toList());
    }

    public List<T> getDatabaseTablesWithoutViews() {
        return databaseTables.stream().filter(t -> !t.isView()).collect(Collectors.toList());
    }

    public T getRandomViewOrBailout() {
        if (getViews().isEmpty()) {
            throw new IgnoreMeException();
        } else {
            return Randomly.fromList(getViews());
        }
    }

    public T getRandomTableNoViewOrBailout() {
        List<T> databaseTablesWithoutViews = getDatabaseTablesWithoutViews();
        if (databaseTablesWithoutViews.isEmpty()) {
            throw new IgnoreMeException();
        }
        return Randomly.fromList(databaseTablesWithoutViews);
    }

    public String getFreeIndexName() {
        int i = 0;
        if (Randomly.getBooleanWithRatherLowProbability()) {
            i = (int) Randomly.getNotCachedInteger(0, 100);
        }
        do {
            String indexName = String.format("i%d", i++);
            boolean indexNameFound = false;
            for (T table : databaseTables) {
                if (table.getIndexes().stream().anyMatch(ind -> ind.getIndexName().contentEquals(indexName))) {
                    indexNameFound = true;
                    break;
                }
            }
            if (!indexNameFound) {
                return indexName;
            }
        } while (true);
    }

    public String getFreeTableName() {
        int i = 0;
        if (Randomly.getBooleanWithRatherLowProbability()) {
            i = (int) Randomly.getNotCachedInteger(0, 100);
        }
        do {
            String tableName = String.format("t%d", i++);
            if (databaseTables.stream().noneMatch(t -> t.getName().equalsIgnoreCase(tableName))) {
                return tableName;
            }
        } while (true);

    }

    public String getFreeViewName() {
        int i = 0;
        if (Randomly.getBooleanWithRatherLowProbability()) {
            i = (int) Randomly.getNotCachedInteger(0, 100);
        }
        do {
            String tableName = String.format("v%d", i++);
            if (databaseTables.stream().noneMatch(t -> t.getName().contentEquals(tableName))) {
                return tableName;
            }
        } while (true);
    }

    public boolean containsTableWithZeroRows(G globalState) {
        return databaseTables.stream().anyMatch(t -> t.getNrRows(globalState) == 0);
    }

}
