package sqlancer.tidb.ast;

public class TiDBPostfixText implements TiDBExpression {
    private final TiDBExpression expr;
    private final String text;
    private final boolean prefix;

    public TiDBPostfixText(TiDBExpression expr, String text, boolean prefix) {
        this.expr = expr;
        this.text = text;
        this.prefix = prefix;
    }

    public TiDBExpression getExpr() {
        return expr;
    }

    public String getText() {
        return text;
    }

    public boolean isPrefix() {
        return prefix;
    }
}
