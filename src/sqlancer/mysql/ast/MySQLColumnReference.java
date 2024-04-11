package sqlancer.mysql.ast;

import sqlancer.mysql.MySQLSchema.MySQLColumn;

public class MySQLColumnReference implements MySQLExpression {

    private final MySQLColumn column;
    private final MySQLConstant value;

    private boolean isRef;

    public MySQLColumnReference(MySQLColumn column, MySQLConstant value) {
        this.column = column;
        this.value = value;
        this.isRef = false;
    }

    public static MySQLColumnReference create(MySQLColumn column, MySQLConstant value) {
        return new MySQLColumnReference(column, value);
    }

    public MySQLColumn getColumn() {
        return column;
    }

    public MySQLConstant getValue() {
        return value;
    }

    @Override
    public MySQLConstant getExpectedValue() {
        return value;
    }

    public boolean isRef() {
        return isRef;
    }

    public MySQLColumnReference setRef(boolean ref) {
        isRef = ref;
        return this;
    }
}
