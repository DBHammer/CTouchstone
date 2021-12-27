package ecnu.db.generator.constraintchain.join;

public enum ConstraintNodeJoinType {
    SEMI_JOIN,
    OUTER_JOIN,
    INNER_JOIN,
    ANTI_JOIN,
    ANTI_SEMI_JOIN;

    public boolean isSemi() {
        return this == SEMI_JOIN || this == ANTI_SEMI_JOIN;
    }

    public boolean isAnti() {
        return this == ANTI_SEMI_JOIN || this == ANTI_JOIN;
    }

    public boolean hasCardinalityConstraint() {
        return this == OUTER_JOIN || this == SEMI_JOIN || this == ANTI_SEMI_JOIN;
    }
}
