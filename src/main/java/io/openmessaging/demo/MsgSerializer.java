package io.openmessaging.demo;

import io.openmessaging.Message;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;

/**
 * MsgSerialize_version_1
 * flag: flag[32]
 * header: header_num[32]| ... key_length[32]|value_length[32]|key[?]|value[?] ...
 * property: property_num[32]| ... key_length[32]|value_length[32]|key[?]|value[?] ...
 * body: body_length[32]| ... body[]
 *
 * MsgSerialize_version_2
 * length_total[32]
 * header: header_num[32]| ... key_length[32]|value_length[32]|key[?]|value[?] ...
 * property: property_num[32]| ... key_length[32]|value_length[32]|key[?]|value[?] ...
 * body: body_length[32]| ... body[]
 *
 * @author Yuchen Chiang
 */
public class MsgSerializer {

    public static void serialize(Message message, ByteBuffer buffer) {

        DefaultBytesMessage msg = (DefaultBytesMessage) message;

        //message.header
        int headerNum = msg.headers().keySet().size();
        buffer.put(int4Byte(headerNum));
        if (headerNum > 0) {
            for (String key: msg.headers().keySet()) {
                buffer.put(int4Byte(key.length()));
                String value = msg.headers().getString(key);
                buffer.put(int4Byte(value.length()));
                buffer.put(key.getBytes());
                buffer.put(value.getBytes());
            }
        }

        //message.properties
        if (msg.properties() != null) {
            int propertiesNum = msg.properties().keySet().size();
            buffer.put(int4Byte(propertiesNum));
            if (propertiesNum > 0) {
                for (String key: msg.properties().keySet()) {
                    buffer.put(int4Byte(key.length()));
                    String value = msg.properties().getString(key);
                    buffer.put(int4Byte(value.length()));
                    buffer.put(key.getBytes());
                    buffer.put(value.getBytes());
                }
            }
        } else {
            buffer.put(int4Byte(0));
        }

        //message.body
        buffer.put(int4Byte(msg.getBody().length));
        buffer.put(msg.getBody());
    }

    public static Message deserialize(ByteBuffer buffer) {

        DefaultBytesMessage msg = new DefaultBytesMessage();

        byte[] numOrLen = new byte[4];
        byte[] cache = new byte[2048];

        //message.header
        buffer.get(numOrLen, 0, 4);
        for (int i = 0; i < byte4Int(numOrLen); i++) {
            buffer.get(cache, 0, 4);
            int keyLen = byte4Int(cache, 0, 4);
            buffer.get(cache, 0 ,4);
            int valueLen = byte4Int(cache, 0, 4);
            buffer.get(cache, 0, keyLen);
            String key = new String(cache, 0, keyLen);
            buffer.get(cache, 0, valueLen);
            String value = new String(cache, 0, valueLen);
            msg.putHeaders(key, value);
        }

        //message.properties
        buffer.get(numOrLen, 0, 4);
        if (byte4Int(numOrLen) > 0) {
            for (int i = 0; i < byte4Int(numOrLen); i++) {
                buffer.get(cache, 0, 4);
                int keyLen = byte4Int(cache, 0, 4);
                buffer.get(cache, 0 ,4);
                int valueLen = byte4Int(cache, 0, 4);
                buffer.get(cache, 0, keyLen);
                String key = new String(cache, 0, keyLen);
                buffer.get(cache, 0, valueLen);
                String value = new String(cache, 0, valueLen);
                msg.putProperties(key, value);
            }
        }

        //message.body
        buffer.get(numOrLen, 0, 4);
        int len = byte4Int(numOrLen);
        byte[] body = new byte[len];
        buffer.get(body, 0 , len);
        msg.setBody(body);

        return (Message) msg;
    }

    public static byte[] serialize2(Message message) {

        DefaultBytesMessage msg = (DefaultBytesMessage) message;
        DefaultKeyValue headers = (DefaultKeyValue) msg.headers();
        DefaultKeyValue properties = (DefaultKeyValue) msg.properties();
        byte[] body = msg.getBody();
        int headersLen = headers.getLength();
        int propertiesLen = properties.getLength();
        int bodyLen = body.length;

        int headerNum = headers.keySet().size();
        int propertiesNum = properties.keySet().size();

        //长度字段包括：本身总长度占用的4个byte是不能包括的，header\prop\body占用的空间，
        //header\prop每个字段的长度所占用的headerNum*4+propNum*4
        //headerNum\propNum\bodyLength占用的总共12byte
        int length = headersLen + propertiesLen + bodyLen + 12 + headerNum * 8 + propertiesNum * 8;

        byte[] data = new byte[length + 4];
        //data数组的位置记录指针
        int pos = 0;

        //首先填入总长度字段
        System.arraycopy(int4Byte(length), 0, data, pos, 4);
        pos += 4;

        //headers
        System.arraycopy(int4Byte(headerNum), 0, data, pos, 4);
        pos += 4;

        for (String key : headers.keySet()) {

            String value = headers.getString(key);
            byte[] keyB = key.getBytes();
            byte[] valueB = value.getBytes();
            int keyL = keyB.length;
            int valueL = valueB.length;

            System.arraycopy(int4Byte(keyL), 0, data, pos, 4);
            pos += 4;
            System.arraycopy(int4Byte(valueL), 0, data, pos, 4);
            pos += 4;
            System.arraycopy(keyB, 0, data, pos, keyL);
            pos += keyL;
            System.arraycopy(valueB, 0 ,data, pos, valueL);
            pos += valueL;
        }

        //properties
        System.arraycopy(int4Byte(propertiesNum), 0, data, pos, 4);
        pos += 4;

        for (String key : properties.keySet()) {

            String value = properties.getString(key);
            byte[] keyB = key.getBytes();
            byte[] valueB = value.getBytes();
            int keyL = keyB.length;
            int valueL = valueB.length;

            System.arraycopy(int4Byte(keyL), 0, data, pos, 4);
            pos += 4;
            System.arraycopy(int4Byte(valueL), 0, data, pos, 4);
            pos += 4;
            System.arraycopy(keyB, 0, data, pos, keyL);
            pos += keyL;
            System.arraycopy(valueB, 0 ,data, pos, valueL);
            pos += valueL;
        }

        System.arraycopy(int4Byte(bodyLen), 0, data, pos, 4);
        pos += 4;

        System.arraycopy(body, 0, data, pos, bodyLen);
        pos += bodyLen;

        return data;
    }

    public static Message deserialize2(byte[] data) {

        DefaultBytesMessage msg = new DefaultBytesMessage();

        int pos = 0;

        //headers
        int numOrLen = byte4Int(data, pos, 4);
        pos += 4;

        for (int i = 0; i < numOrLen; i++) {

            int keyL = byte4Int(data, pos, 4);
            pos += 4;
            int valueL = byte4Int(data, pos, 4);
            pos += 4;

            String key = new String(data, pos, keyL);
            pos += keyL;
            String value = new String(data, pos, valueL);
            pos += valueL;

            msg.putHeaders(key, value);
        }

        //properties
        numOrLen = byte4Int(data, pos, 4);
        pos += 4;

        for (int i = 0; i < numOrLen; i++) {

            int keyL = byte4Int(data, pos, 4);
            pos += 4;
            int valueL = byte4Int(data, pos, 4);
            pos += 4;

            String key = new String(data, pos, keyL);
            pos += keyL;
            String value = new String(data, pos, valueL);
            pos += valueL;

            msg.putProperties(key, value);
        }

        //body
        numOrLen = byte4Int(data, pos, 4);
        pos += 4;
        byte[] body = new byte[numOrLen];
        System.arraycopy(data, pos, body, 0, numOrLen);

        msg.setBody(body);

        return msg;
    }

    public static int byte4Int(byte[] b) {
        return
                (b[3] & 0xFF) |
                        (b[2] & 0xFF) << 8 |
                        (b[1] & 0xFF) << 16 |
                        (b[0] & 0xFF)  << 24;
    }

    private static int byte4Int(byte[] b, int offset, int length) {
        return
                (b[offset + length -1] & 0xFF) |
                        (b[offset + length -2] & 0xFF) << 8 |
                        (b[offset + length -3] & 0xFF) << 16 |
                        (b[offset] & 0xFF) << 24;
    }

    private static byte[] int4Byte(int i) {
        return new byte[] {
                (byte) ((i >> 24) & 0xFF),
                (byte) ((i >> 16) & 0xFF),
                (byte) ((i >> 8) & 0xFF),
                (byte) (i & 0xFF)
        };
    }
}