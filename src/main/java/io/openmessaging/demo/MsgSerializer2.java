package io.openmessaging.demo;

import io.openmessaging.Message;

import java.util.Set;

/**
 * @author Yuchen Chiang
 */
public class MsgSerializer2 {

    /**
     *
     *
     * @author Yuche Chiang
     */
    public static byte[] serialize2(Message message) {

        byte[] body = ((DefaultBytesMessage) message).getBody();
        DefaultKeyValue headers = (DefaultKeyValue) message.headers();
        DefaultKeyValue property = (DefaultKeyValue) message.properties();

        int sumLength = 3 + body.length + headers.getLength() + property.getLength();

        byte[] data;
        int pos = 0;


        if (sumLength > 127) {
            sumLength += 2;
            data = new byte[sumLength + 4];
            pos = intToByteArray(sumLength, data, pos);
            pos = intTo3ByteArray(body.length, data, pos);

            for (int i = 0, len = body.length; i < len; i++) {
                data[pos++] = body[i];
            }

            Set<String> keys = headers.keySet();

            data[pos++] = (byte) keys.size();
            for (String key : keys) {
                String value = headers.getString(key);
                data[pos++] = (byte) key.length();
                for (int i = 0, len = key.length(); i < len; i++) {
                    data[pos++] = (byte) (key.charAt(i));
                }
                if (value.length() > 63) {
                    pos = intTo2ByteArray(value.length(), data, pos);
                } else {
                    data[pos++] = (byte) value.length();
                }
                for (int i = 0, len = value.length(); i < len; i++) {
                    data[pos++] = (byte) (value.charAt(i));
                }
            }

            keys = property.keySet();
            data[pos++] = (byte) keys.size();
            for (String key : keys) {
                String value = property.getString(key);
                data[pos++] = (byte) key.length();
                for (int i = 0, len = key.length(); i < len; i++) {
                    data[pos++] = (byte) (key.charAt(i));
                }
                if (value.length() > 63) {
                    pos = intTo2ByteArray(value.length(), data, pos);
                } else {
                    data[pos++] = (byte) value.length();
                }
                for (int i = 0, len = value.length(); i < len; i++) {
                    data[pos++] = (byte) (value.charAt(i));
                }
            }


        } else {
            //
            data = new byte[sumLength + 4];
            //向data中填充4字节长度的sumLength
            pos = intToByteArray(sumLength, data, pos);
            //以1字节的长度记录body[]长度，因为sumLength<127所以可以安全使用1字节储存body长度
            data[pos++] = (byte) body.length;
            //将body的内容拷贝到data[]
            for (int i = 0, len = body.length; i < len; i++) {
                data[pos++] = body[i];
            }

            Set<String> keys = headers.keySet();
            //以1字节储存headers的键值对数量
            data[pos++] = (byte) keys.size();
            //对于headers中的key和value，逐字节地拷贝到data[]中
            for (String key : keys) {
                String value = headers.getString(key);
                data[pos++] = (byte) key.length();
                for (int i = 0, len = key.length(); i < len; i++) {
                    data[pos++] = (byte) (key.charAt(i));
                }
                data[pos++] = (byte) value.length();
                for (int i = 0, len = value.length(); i < len; i++) {
                    data[pos++] = (byte) (value.charAt(i));
                }
            }

            keys = property.keySet();
            data[pos++] = (byte) keys.size();
            for (String key : keys) {
                String value = property.getString(key);
                data[pos++] = (byte) key.length();
                for (int i = 0, len = key.length(); i < len; i++) {
                    data[pos++] = (byte) (key.charAt(i));
                }
                data[pos++] = (byte) value.length();
                for (int i = 0, len = value.length(); i < len; i++) {
                    data[pos++] = (byte) (value.charAt(i));
                }
            }
        }
        return data;
    }

    public static Message deserialize2(byte[] result) {
        int sumLength = result.length;
        int pos = 0;
        Message message;

        if (sumLength > 127) {
            int bodyLength = byte3ArrayToInt(result, pos);
            pos += 3;
            byte[] body = new byte[bodyLength];
            for (int i = 0; i < bodyLength; ++i) {
                body[i] = result[pos++];
            }
            message = new DefaultBytesMessage(body);
            int headerLength = result[pos++];

            int keyLength;
            int valueLength;
            for (int i = 0; i < headerLength; ++i) {
                keyLength = result[pos++];

                int keyStart = pos;
                pos += keyLength;
                if (result[pos] > 63) {
                    valueLength = byte2ArrayToInt(result, pos);
                    pos += 2;
                } else {
                    valueLength = result[pos++];
                }

                int valueStart = pos;
                pos += valueLength;
                message.putHeaders(new String(result, keyStart, keyLength), new String(result, valueStart, valueLength));
            }

            headerLength = result[pos++];

            for (int i = 0; i < headerLength; ++i) {
                keyLength = result[pos++];

                int keyStart = pos;
                pos += keyLength;
                if (result[pos] > 63) {
                    valueLength = byte2ArrayToInt(result, pos);
                    pos += 2;
                } else {
                    valueLength = result[pos++];
                }

                int valueStart = pos;
                pos += valueLength;
                message.putProperties(new String(result, keyStart, keyLength), new String(result, valueStart, valueLength));
            }

        } else {
            int bodyLength = result[pos++];
            byte[] body = new byte[bodyLength];
            for (int i = 0; i < bodyLength; ++i) {
                body[i] = result[pos++];
            }
            message = new DefaultBytesMessage(body);
            int headerLength = result[pos++];

            int keyLength;
            int valueLength;
            for (int i = 0; i < headerLength; ++i) {
                keyLength = result[pos++];

                int keyStart = pos;
                pos += keyLength;
                valueLength = result[pos++];

                int valueStart = pos;
                pos += valueLength;
                message.putHeaders(new String(result, keyStart, keyLength), new String(result, valueStart, valueLength));
            }

            headerLength = result[pos++];

            for (int i = 0; i < headerLength; ++i) {
                keyLength = result[pos++];

                int keyStart = pos;
                pos += keyLength;
                valueLength = result[pos++];

                int valueStart = pos;
                pos += valueLength;
                message.putProperties(new String(result, keyStart, keyLength), new String(result, valueStart, valueLength));
            }

        }

        return message;
    }

    private static int intToByteArray(int b, byte[] result, int pos) {
        result[pos + 3] = (byte) b;
        for (int i = 2; i > -1; i--) {
            b >>= 8;
            result[pos + i] = (byte) b;
        }
        return pos + 4;
    }

    private static int intTo2ByteArray(int b, byte[] result, int pos) {
        result[pos + 1] = (byte) b;
        result[pos] = (byte) ((b >> 8 & 0xff) + 64);
        return pos + 2;
    }

    private static int intTo3ByteArray(int x, byte[] result, int pos) {
        result[pos + 2] = (byte) x;
        x >>= 8;
        result[pos + 1] = (byte) x;
        x >>= 8;
        result[pos] = (byte) x;
        return pos + 3;
    }

    private static int byte2ArrayToInt(byte[] res, int pos) {
        int targets = (res[pos + 1] & 0xff) | ((res[pos] - 64) << 8);//extra 64 process
        return targets;
    }

    private static int byte3ArrayToInt(byte[] res, int pos) {
        int targets = (res[pos + 2] & 0xff) | ((res[pos + 1] << 8) & 0xff00) | (res[pos] << 16);
        return targets;
    }
}
