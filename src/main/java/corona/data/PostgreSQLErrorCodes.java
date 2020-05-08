package corona.data;

public enum PostgreSQLErrorCodes {

    NOT_NULL_VIOLATION (23502),
    FOREIGN_KEY_VIOLATION(23503),
    UNIQUE_VIOLATION(23505),
    CHECK_VIOLATION(23514);


    private final int errorCode;
    PostgreSQLErrorCodes(int errorCode)
    {
        this.errorCode = errorCode;
    }
    public int getValue()
    {
        return errorCode;
    }
}
