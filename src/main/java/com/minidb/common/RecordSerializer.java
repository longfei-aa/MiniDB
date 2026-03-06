package com.minidb.common;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * DataRecord 序列化/反序列化工具类
 *
 * 格式说明：
 * - 隐藏列：dbRowId (8字节) + dbTrxId (8字节) + dbRollPtr (8字节) + deleted (1字节)
 * - 字段数量：fieldCount (4字节)
 * - 每个字段：
 *   - fieldName长度 (4字节) + fieldName (变长)
 *   - 类型标记 (1字节): 0=NULL, 1=Integer, 2=String, 3=Long
 *   - 字段值 (变长)
 */
public class RecordSerializer {

    /**
     * 序列化 DataRecord 到字节数组
     */
    public static byte[] serialize(DataRecord record) {
        if (record == null) {
            return new byte[0];
        }

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(baos)) {

            // 1. 写入隐藏列
            dos.writeLong(record.getDbRowId());
            dos.writeLong(record.getDbTrxId());
            dos.writeLong(record.getDbRollPtr());
            dos.writeBoolean(record.isDeleted());

            // 2. 写入用户数据
            Map<String, Object> values = record.getValues();
            dos.writeInt(values.size());  // 字段数量

            for (Map.Entry<String, Object> entry : values.entrySet()) {
                // 写入字段名
                String fieldName = entry.getKey();
                byte[] fieldNameBytes = fieldName.getBytes(StandardCharsets.UTF_8);
                dos.writeInt(fieldNameBytes.length);
                dos.write(fieldNameBytes);

                // 写入字段值
                Object value = entry.getValue();
                if (value == null) {
                    dos.writeByte(0);  // NULL
                } else if (value instanceof Integer) {
                    dos.writeByte(1);  // Integer
                    dos.writeInt((Integer) value);
                } else if (value instanceof String) {
                    dos.writeByte(2);  // String
                    String strValue = (String) value;
                    byte[] strValueBytes = strValue.getBytes(StandardCharsets.UTF_8);
                    dos.writeInt(strValueBytes.length);
                    dos.write(strValueBytes);
                } else if (value instanceof Long) {
                    dos.writeByte(3);  // Long
                    dos.writeLong((Long) value);
                } else {
                    // 不支持的类型，当作字符串处理
                    dos.writeByte(2);  // String
                    String strValue = value.toString();
                    byte[] strValueBytes = strValue.getBytes(StandardCharsets.UTF_8);
                    dos.writeInt(strValueBytes.length);
                    dos.write(strValueBytes);
                }
            }

            dos.flush();
            return baos.toByteArray();

        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize DataRecord", e);
        }
    }

    /**
     * 从字节数组反序列化 DataRecord
     */
    public static DataRecord deserialize(byte[] data) {
        if (data == null || data.length == 0) {
            return null;
        }

        try (ByteArrayInputStream bais = new ByteArrayInputStream(data);
             DataInputStream dis = new DataInputStream(bais)) {

            DataRecord record = new DataRecord();

            // 1. 读取隐藏列
            record.setDbRowId(dis.readLong());
            record.setDbTrxId(dis.readLong());
            record.setDbRollPtr(dis.readLong());
            record.setDeleted(dis.readBoolean());

            // 2. 读取用户数据
            int fieldCount = dis.readInt();

            for (int i = 0; i < fieldCount; i++) {
                // 读取字段名
                int nameLength = dis.readInt();
                byte[] nameBytes = new byte[nameLength];
                dis.readFully(nameBytes);
                String fieldName = new String(nameBytes, StandardCharsets.UTF_8);

                // 读取字段值
                byte typeMarker = dis.readByte();
                Object value;

                switch (typeMarker) {
                    case 1:  // Integer
                        value = dis.readInt();
                        break;
                    case 2:  // String
                        int valueLength = dis.readInt();
                        byte[] valueBytes = new byte[valueLength];
                        dis.readFully(valueBytes);
                        value = new String(valueBytes, StandardCharsets.UTF_8);
                        break;
                    case 3:  // Long
                        value = dis.readLong();
                        break;
                    default:  // NULL
                        value = null;
                }

                record.setValue(fieldName, value);
            }

            return record;

        } catch (IOException e) {
            throw new RuntimeException("Failed to deserialize DataRecord", e);
        }
    }
}
