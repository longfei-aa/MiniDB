package com.minidb.sql;

/**
 * SQL词法单元
 */
public class Token {
    private TokenType type;
    private String value;

    public enum TokenType {
        // 关键字
        CREATE, TABLE, INSERT, INTO, VALUES, SELECT, FROM, WHERE,
        UPDATE, SET, DELETE, AND, OR, NOT, NULL, PRIMARY, KEY, INT, VARCHAR,
        BEGIN, COMMIT, ROLLBACK,
        INDEX, ON,  // 索引相关关键字

        // 符号
        LPAREN, RPAREN, COMMA, SEMICOLON, STAR, EQUAL, LESS, GREATER,
        LESS_EQUAL, GREATER_EQUAL, NOT_EQUAL,

        // 字面量
        IDENTIFIER, NUMBER, STRING,

        // 其他
        EOF, UNKNOWN
    }

    public Token(TokenType type, String value) {
        this.type = type;
        this.value = value;
    }

    public TokenType getType() {
        return type;
    }

    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
        return String.format("Token(%s, '%s')", type, value);
    }
}
