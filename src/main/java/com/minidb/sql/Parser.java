package com.minidb.sql;

import com.minidb.common.Column;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * SQL语法分析器（递归下降解析）
 */
public class Parser {
    private List<Token> tokens;
    private int position;
    private Token currentToken;

    public Parser(String sql) {
        Lexer lexer = new Lexer(sql);
        this.tokens = lexer.tokenize();
        this.position = 0;
        this.currentToken = tokens.get(0);
    }

    private void advance() {
        position++;
        if (position < tokens.size()) {
            currentToken = tokens.get(position);
        }
    }

    private void expect(Token.TokenType type) throws Exception {
        if (currentToken.getType() != type) {
            throw new Exception("Expected " + type + " but got " + currentToken.getType());
        }
        advance();
    }

    public Statement parse() throws Exception {
        Statement statement;
        switch (currentToken.getType()) {
            case CREATE:
                statement = parseCreate();
                break;
            case INSERT:
                statement = parseInsert();
                break;
            case SELECT:
                statement = parseSelect();
                break;
            case UPDATE:
                statement = parseUpdate();
                break;
            case DELETE:
                statement = parseDelete();
                break;
            case BEGIN:
                statement = parseBegin();
                break;
            case COMMIT:
                statement = parseCommit();
                break;
            case ROLLBACK:
                statement = parseRollback();
                break;
            default:
                throw new Exception("Unsupported statement: " + currentToken.getType());
        }

        // 允许可选分号，但不允许尾随垃圾token
        if (currentToken.getType() == Token.TokenType.SEMICOLON) {
            advance();
        }
        if (currentToken.getType() != Token.TokenType.EOF) {
            throw new Exception("Unexpected token after statement: " + currentToken.getType());
        }
        return statement;
    }

    /**
     * 解析CREATE语句（TABLE或INDEX）
     */
    private Statement parseCreate() throws Exception {
        expect(Token.TokenType.CREATE);

        // 判断是CREATE TABLE还是CREATE INDEX
        if (currentToken.getType() == Token.TokenType.TABLE) {
            return parseCreateTableBody();
        } else if (currentToken.getType() == Token.TokenType.INDEX) {
            return parseCreateIndex();
        } else {
            throw new Exception("Expected TABLE or INDEX after CREATE, got: " + currentToken.getType());
        }
    }

    /**
     * 解析CREATE TABLE语句主体（已消费CREATE关键字）
     * CREATE TABLE table_name (col1 INT PRIMARY KEY, col2 VARCHAR(255), ...)
     */
    private Statement.CreateTableStatement parseCreateTableBody() throws Exception {
        expect(Token.TokenType.TABLE);

        String tableName = currentToken.getValue();
        expect(Token.TokenType.IDENTIFIER);

        expect(Token.TokenType.LPAREN);

        List<Column> columns = new ArrayList<>();
        while (currentToken.getType() != Token.TokenType.RPAREN) {
            String columnName = currentToken.getValue();
            expect(Token.TokenType.IDENTIFIER);

            Column.DataType dataType;
            int length = 0;

            switch (currentToken.getType()) {
                case INT:
                    dataType = Column.DataType.INT;
                    length = 4;
                    advance();
                    break;
                case VARCHAR:
                    dataType = Column.DataType.VARCHAR;
                    advance();
                    if (currentToken.getType() == Token.TokenType.LPAREN) {
                        advance();
                        length = Integer.parseInt(currentToken.getValue());
                        expect(Token.TokenType.NUMBER);
                        expect(Token.TokenType.RPAREN);
                    } else {
                        length = 255;
                    }
                    break;
                default:
                    throw new Exception("Unsupported data type: " + currentToken.getType());
            }

            boolean primaryKey = false;
            if (currentToken.getType() == Token.TokenType.PRIMARY) {
                advance();
                expect(Token.TokenType.KEY);
                primaryKey = true;
            }

            Column column = new Column(columnName, dataType, length, true, primaryKey);
            columns.add(column);

            if (currentToken.getType() == Token.TokenType.COMMA) {
                advance();
            } else {
                break;
            }
        }

        expect(Token.TokenType.RPAREN);

        return new Statement.CreateTableStatement(tableName, columns);
    }

    /**
     * 解析CREATE INDEX语句
     * CREATE INDEX index_name ON table_name (column1, column2, ...)
     */
    private Statement.CreateIndexStatement parseCreateIndex() throws Exception {
        expect(Token.TokenType.INDEX);

        String indexName = currentToken.getValue();
        expect(Token.TokenType.IDENTIFIER);

        expect(Token.TokenType.ON);

        String tableName = currentToken.getValue();
        expect(Token.TokenType.IDENTIFIER);

        expect(Token.TokenType.LPAREN);

        List<String> columnNames = new ArrayList<>();
        while (currentToken.getType() != Token.TokenType.RPAREN) {
            String columnName = currentToken.getValue();
            expect(Token.TokenType.IDENTIFIER);
            columnNames.add(columnName);

            if (currentToken.getType() == Token.TokenType.COMMA) {
                advance();
            } else {
                break;
            }
        }

        expect(Token.TokenType.RPAREN);

        if (columnNames.isEmpty()) {
            throw new Exception("Index must have at least one column");
        }

        return new Statement.CreateIndexStatement(indexName, tableName, columnNames);
    }

    /**
     * 解析INSERT语句
     * INSERT INTO table_name VALUES (val1, val2, ...)
     */
    private Statement.InsertStatement parseInsert() throws Exception {
        expect(Token.TokenType.INSERT);
        expect(Token.TokenType.INTO);

        String tableName = currentToken.getValue();
        expect(Token.TokenType.IDENTIFIER);

        expect(Token.TokenType.VALUES);
        expect(Token.TokenType.LPAREN);

        List<Object> values = new ArrayList<>();
        while (currentToken.getType() != Token.TokenType.RPAREN) {
            if (currentToken.getType() == Token.TokenType.NUMBER) {
                values.add(Integer.parseInt(currentToken.getValue()));
                advance();
            } else if (currentToken.getType() == Token.TokenType.STRING) {
                values.add(currentToken.getValue());
                advance();
            } else {
                throw new Exception("Unexpected value type: " + currentToken.getType());
            }

            if (currentToken.getType() == Token.TokenType.COMMA) {
                advance();
            } else {
                break;
            }
        }

        expect(Token.TokenType.RPAREN);

        return new Statement.InsertStatement(tableName, null, values);
    }

    /**
     * 解析SELECT语句
     * SELECT * FROM table_name WHERE col = value
     */
    private Statement.SelectStatement parseSelect() throws Exception {
        expect(Token.TokenType.SELECT);

        List<String> columns = new ArrayList<>();
        if (currentToken.getType() == Token.TokenType.STAR) {
            columns.add("*");
            advance();
        } else {
            while (currentToken.getType() == Token.TokenType.IDENTIFIER) {
                columns.add(currentToken.getValue());
                advance();
                if (currentToken.getType() == Token.TokenType.COMMA) {
                    advance();
                } else {
                    break;
                }
            }
        }

        expect(Token.TokenType.FROM);
        String tableName = currentToken.getValue();
        expect(Token.TokenType.IDENTIFIER);

        Statement.WhereClause whereClause = null;
        if (currentToken.getType() == Token.TokenType.WHERE) {
            whereClause = parseWhereClause();
        }

        return new Statement.SelectStatement(tableName, columns, whereClause);
    }

    /**
     * 解析WHERE子句
     */
    private Statement.WhereClause parseWhereClause() throws Exception {
        expect(Token.TokenType.WHERE);

        String column = currentToken.getValue();
        expect(Token.TokenType.IDENTIFIER);

        String operator;
        switch (currentToken.getType()) {
            case EQUAL:
                operator = "=";
                break;
            case LESS:
                operator = "<";
                break;
            case GREATER:
                operator = ">";
                break;
            case LESS_EQUAL:
                operator = "<=";
                break;
            case GREATER_EQUAL:
                operator = ">=";
                break;
            case NOT_EQUAL:
                operator = "!=";
                break;
            default:
                throw new Exception("Unsupported operator: " + currentToken.getType());
        }
        advance();

        Object value;
        if (currentToken.getType() == Token.TokenType.NUMBER) {
            value = Integer.parseInt(currentToken.getValue());
            advance();
        } else if (currentToken.getType() == Token.TokenType.STRING) {
            value = currentToken.getValue();
            advance();
        } else {
            throw new Exception("Unexpected value type: " + currentToken.getType());
        }

        return new Statement.WhereClause(column, operator, value);
    }

    /**
     * 解析UPDATE语句
     * UPDATE table_name SET col1 = val1 WHERE col2 = val2
     */
    private Statement.UpdateStatement parseUpdate() throws Exception {
        expect(Token.TokenType.UPDATE);

        String tableName = currentToken.getValue();
        expect(Token.TokenType.IDENTIFIER);

        expect(Token.TokenType.SET);

        Map<String, Object> updates = new HashMap<>();
        while (true) {
            String column = currentToken.getValue();
            expect(Token.TokenType.IDENTIFIER);
            expect(Token.TokenType.EQUAL);

            Object value;
            if (currentToken.getType() == Token.TokenType.NUMBER) {
                value = Integer.parseInt(currentToken.getValue());
                advance();
            } else if (currentToken.getType() == Token.TokenType.STRING) {
                value = currentToken.getValue();
                advance();
            } else {
                throw new Exception("Unexpected value type: " + currentToken.getType());
            }

            updates.put(column, value);

            if (currentToken.getType() == Token.TokenType.COMMA) {
                advance();
            } else {
                break;
            }
        }

        Statement.WhereClause whereClause = null;
        if (currentToken.getType() == Token.TokenType.WHERE) {
            whereClause = parseWhereClause();
        }

        return new Statement.UpdateStatement(tableName, updates, whereClause);
    }

    /**
     * 解析DELETE语句
     * DELETE FROM table_name WHERE col = value
     */
    private Statement.DeleteStatement parseDelete() throws Exception {
        expect(Token.TokenType.DELETE);
        expect(Token.TokenType.FROM);

        String tableName = currentToken.getValue();
        expect(Token.TokenType.IDENTIFIER);

        Statement.WhereClause whereClause = null;
        if (currentToken.getType() == Token.TokenType.WHERE) {
            whereClause = parseWhereClause();
        }

        return new Statement.DeleteStatement(tableName, whereClause);
    }

    /**
     * 解析BEGIN语句
     * BEGIN 或 BEGIN TRANSACTION
     */
    private Statement.BeginStatement parseBegin() throws Exception {
        expect(Token.TokenType.BEGIN);
        // 可选的 TRANSACTION 关键字
        if (currentToken.getType() == Token.TokenType.IDENTIFIER &&
            currentToken.getValue().equalsIgnoreCase("TRANSACTION")) {
            advance();
        }
        return new Statement.BeginStatement();
    }

    /**
     * 解析COMMIT语句
     * COMMIT 或 COMMIT TRANSACTION
     */
    private Statement.CommitStatement parseCommit() throws Exception {
        expect(Token.TokenType.COMMIT);
        // 可选的 TRANSACTION 关键字
        if (currentToken.getType() == Token.TokenType.IDENTIFIER &&
            currentToken.getValue().equalsIgnoreCase("TRANSACTION")) {
            advance();
        }
        return new Statement.CommitStatement();
    }

    /**
     * 解析ROLLBACK语句
     * ROLLBACK 或 ROLLBACK TRANSACTION
     */
    private Statement.RollbackStatement parseRollback() throws Exception {
        expect(Token.TokenType.ROLLBACK);
        // 可选的 TRANSACTION 关键字
        if (currentToken.getType() == Token.TokenType.IDENTIFIER &&
            currentToken.getValue().equalsIgnoreCase("TRANSACTION")) {
            advance();
        }
        return new Statement.RollbackStatement();
    }
}
