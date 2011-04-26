package com.teradata.sqlparser.node;


public final class JoinPart implements java.io.Serializable, Cloneable {

	static final long serialVersionUID = -1664565759669808084L;

	/**
	 * The type of join. Either LEFT_OUTER_JOIN, RIGHT_OUTER_JOIN,
	 * FULL_OUTER_JOIN, INNER_JOIN.
	 */
	int type;

	/**
	 * The expression that we are joining on (eg. ON clause in SQL). If there is
	 * no ON expression (such as in the case of natural joins) then this is null.
	 */
	Expression on_expression;

	/**
	 * Constructs the JoinPart.
	 */
	public JoinPart(int type, Expression on_expression) {
		this.type = type;
		this.on_expression = on_expression;
	}

	public JoinPart(int type) {
		this(type, null);
	}

	public Object clone() throws CloneNotSupportedException {
		JoinPart v = (JoinPart) super.clone();
		if (on_expression != null) {
			v.on_expression = (Expression) on_expression.clone();
		}
		return v;
	}

	public Expression getExpression() {
		return this.on_expression;
	}
}
