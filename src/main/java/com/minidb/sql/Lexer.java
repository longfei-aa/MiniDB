package com.minidb.sql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * SQL词法分析器
 */
public class Lexer {
    private String sql;
    private int position;
    private char currentChar;

    private static final Map<String, Token.TokenType> KEYWORDS = new HashMap<>();

    static {
        KEYWORDS.put("CREATE", Token.TokenType.CREATE);
        KEYWORDS.put("TABLE", Token.TokenType.TABLE);
        KEYWORDS.put("INSERT", Token.TokenType.INSERT);
        KEYWORDS.put("INTO", Token.TokenType.INTO);
        KEYWORDS.put("VALUES", Token.TokenType.VALUES);
        KEYWORDS.put("SELECT", Token.TokenType.SELECT);
        KEYWORDS.put("FROM", Token.TokenType.FROM);
        KEYWORDS.put("WHERE", Token.TokenType.WHERE);
        KEYWORDS.put("UPDATE", Token.TokenType.UPDATE);
        KEYWORDS.put("SET", Token.TokenType.SET);
        KEYWORDS.put("DELETE", Token.TokenType.DELETE);
        KEYWORDS.put("AND", Token.TokenType.AND);
        KEYWORDS.put("OR", Token.TokenType.OR);
        KEYWORDS.put("NOT", Token.TokenType.NOT);
        KEYWORDS.put("NULL", Token.TokenType.NULL);
        KEYWORDS.put("PRIMARY", Token.TokenType.PRIMARY);
        KEYWORDS.put("KEY", Token.TokenType.KEY);
        KEYWORDS.put("INT", Token.TokenType.INT);
        KEYWORDS.put("VARCHAR", Token.TokenType.VARCHAR);
        KEYWORDS.put("BEGIN", Token.TokenType.BEGIN);
        KEYWORDS.put("COMMIT", Token.TokenType.COMMIT);
        KEYWORDS.put("ROLLBACK", Token.TokenType.ROLLBACK);
        KEYWORDS.put("INDEX", Token.TokenType.INDEX);
        KEYWORDS.put("ON", Token.TokenType.ON);
    }

    public Lexer(String sql) {
        this.sql = sql;
        this.position = 0;
        this.currentChar = sql.length() > 0 ? sql.charAt(0) : '\0';
    }

    private void advance() {
        position++;
        if (position < sql.length()) {
            currentChar = sql.charAt(position);
        } else {
            currentChar = '\0';
        }
    }

    private void skipWhitespace() {
        while (currentChar != '\0' && Character.isWhitespace(currentChar)) {
            advance();
        }
    }

    private String readNumber() {
        StringBuilder sb = new StringBuilder();
        while (currentChar != '\0' && Character.isDigit(currentChar)) {
            sb.append(currentChar);
            advance();
        }
        return sb.toString();
    }

    private String readIdentifier() {
        StringBuilder sb = new StringBuilder();
        while (currentChar != '\0' && (Character.isLetterOrDigit(currentChar) || currentChar == '_')) {
            sb.append(currentChar);
            advance();
        }
        return sb.toString();
    }

    private String readString() {
        StringBuilder sb = new StringBuilder();
        advance();  // 跳过开始的引号

        while (currentChar != '\0' && currentChar != '\'') {
            sb.append(currentChar);
            advance();
        }

        if (currentChar == '\'') {
            advance();  // 跳过结束的引号
        }

        return sb.toString();
    }

    public Token nextToken() {
        while (currentChar != '\0') {
            if (Character.isWhitespace(currentChar)) {
                skipWhitespace();
                continue;
            }

            if (Character.isDigit(currentChar)) {
                return new Token(Token.TokenType.NUMBER, readNumber());
            }

            if (Character.isLetter(currentChar) || currentChar == '_') {
                String identifier = readIdentifier();
                String upperIdentifier = identifier.toUpperCase();
                Token.TokenType type = KEYWORDS.getOrDefault(upperIdentifier, Token.TokenType.IDENTIFIER);
                return new Token(type, identifier);
            }

            if (currentChar == '\'') {
                return new Token(Token.TokenType.STRING, readString());
            }

            // 符号处理
            switch (currentChar) {
                case '(':
                    advance();
                    return new Token(Token.TokenType.LPAREN, "(");
                case ')':
                    advance();
                    return new Token(Token.TokenType.RPAREN, ")");
                case ',':
                    advance();
                    return new Token(Token.TokenType.COMMA, ",");
                case ';':
                    advance();
                    return new Token(Token.TokenType.SEMICOLON, ";");
                case '*':
                    advance();
                    return new Token(Token.TokenType.STAR, "*");
                case '=':
                    advance();
                    return new Token(Token.TokenType.EQUAL, "=");
                case '<':
                    advance();
                    if (currentChar == '=') {
                        advance();
                        return new Token(Token.TokenType.LESS_EQUAL, "<=");
                    } else if (currentChar == '>') {
                        advance();
                        return new Token(Token.TokenType.NOT_EQUAL, "<>");
                    }
                    return new Token(Token.TokenType.LESS, "<");
                case '>':
                    advance();
                    if (currentChar == '=') {
                        advance();
                        return new Token(Token.TokenType.GREATER_EQUAL, ">=");
                    }
                    return new Token(Token.TokenType.GREATER, ">");
                case '!':
                    advance();
                    if (currentChar == '=') {
                        advance();
                        return new Token(Token.TokenType.NOT_EQUAL, "!=");
                    }
                    break;
            }

            advance();
            return new Token(Token.TokenType.UNKNOWN, String.valueOf(currentChar));
        }

        return new Token(Token.TokenType.EOF, "");
    }

    public List<Token> tokenize() {
        List<Token> tokens = new ArrayList<>();
        Token token;
        do {
            token = nextToken();
            tokens.add(token);
        } while (token.getType() != Token.TokenType.EOF);
        return tokens;
    }
}
