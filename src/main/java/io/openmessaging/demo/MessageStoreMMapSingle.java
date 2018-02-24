package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Yuchen Chiang
 */
public class MessageStoreMMapSingle {

    private static final MessageStoreMMapSingle INSTANCE = new MessageStoreMMapSingle();

    public static MessageStoreMMapSingle getInstance() {
        return INSTANCE;
    }

    private static final int LOG_FILE_MAX = 64 * 1024 * 1024;

    private HashMap<String, MappedByteBuffer> bufLocW = new HashMap<>();

    private ThreadLocal<Map<String, ByteBuffer>> bufLocR = new ThreadLocal<Map<String, ByteBuffer>>(){
        @Override
        protected Map<String, ByteBuffer> initialValue() {
            return new HashMap<>();
        }
    };

    public void putMessage(String bucket, Message msg, KeyValue props) throws IOException {

        String storePath = props.getString("STORE_PATH");

        MappedByteBuffer buf;

        byte[] data = MsgSerializer.serialize2(msg);

        synchronized (MessageStoreMMapSingle.class) {
            buf = bufLocW.get(bucket);
            if (buf == null) {
                buf = new RandomAccessFile(storePath + File.separator + bucket + ".log", "rw").getChannel().map(FileChannel.MapMode.READ_WRITE, 0, LOG_FILE_MAX);
                bufLocW.put(bucket, buf);
            }
            buf.put(data);
        }

    }

    public Message pullMessage(String bucket, KeyValue props) throws IOException {

        String storePath = props.getString("STORE_PATH");

        Map<String, ByteBuffer> bufR = bufLocR.get();

        ByteBuffer buf;
        buf = bufR.get(bucket);
        if (buf == null) {
            buf = new RandomAccessFile(storePath + File.separator + bucket + ".log", "r").getChannel().map(FileChannel.MapMode.READ_ONLY, 0, LOG_FILE_MAX);
            bufR.put(bucket, buf);
        }

        int length = buf.getInt();
        if (length == 0) {
            return null;
        }
        byte[] data = new byte[length];
        buf.get(data);

        return MsgSerializer.deserialize2(data);
    }

    public void flush(KeyValue props) throws IOException {
    }
}
