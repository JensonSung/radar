package sqlancer.mysql.ast;


public class MySQLPostfixText implements MySQLExpression {
    private final MySQLExpression expr;
    private final String text;
    private final boolean prefix;

    public MySQLPostfixText(MySQLExpression expr, String text, boolean prefix) {
        this.expr = expr;
        this.text = text;
        this.prefix = prefix;
    }

    public MySQLExpression getExpr() {
        return expr;
    }

    public String getText() {
        return text;
    }
    public boolean isPrefix() {
        return prefix;
    }
}
