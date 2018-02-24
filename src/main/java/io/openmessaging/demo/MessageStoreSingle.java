package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Yuchen Chiang
 */
public class MessageStoreSingle {

    private static final MessageStoreSingle INSTANCE = new MessageStoreSingle();

    public static MessageStoreSingle getInstance() {
        return INSTANCE;
    }

    private static final int LOG_FILE_MAX = 64 * 1024 * 1024;

    private HashMap<String, FileChannel> bufLocW = new HashMap<>();

    private ThreadLocal<Map<String, FileChannel>> bufLocR = new ThreadLocal<Map<String, FileChannel>>(){
        @Override
        protected Map<String, FileChannel> initialValue() {
            return new HashMap<>();
        }
    };

    private Lock lock = new ReentrantLock();

    public void putMessage(String bucket, Message msg, KeyValue props) throws IOException {

        String storePath = props.getString("STORE_PATH");

        FileChannel fc;

        byte[] data = MsgSerializer.serialize2(msg);
        ByteBuffer buf = ByteBuffer.wrap(data);

        synchronized (MessageStoreMMapSingle.class) {
            fc = bufLocW.get(bucket);
            if (fc == null) {
                fc = new RandomAccessFile(storePath + File.separator + bucket + ".log", "rw").getChannel();
                bufLocW.put(bucket, fc);
            }
            fc.write(buf);
        }

    }

    public Message pullMessage(String bucket, KeyValue props) throws IOException {

        String storePath = props.getString("STORE_PATH");

        Map<String, FileChannel> bufR = bufLocR.get();

        FileChannel fc;
        fc = bufR.get(bucket);
        if (fc == null) {
            fc = new RandomAccessFile(storePath + File.separator + bucket + ".log", "r").getChannel();
            bufR.put(bucket, fc);
        }

        ByteBuffer lenBuf = ByteBuffer.allocate(4);
        fc.read(lenBuf);
        int length = MsgSerializer.byte4Int(lenBuf.array());
        if (length == 0) {
            return null;
        }
        ByteBuffer buf = ByteBuffer.allocate(length);
        fc.read(buf);
        byte[] data = buf.array();

        return MsgSerializer.deserialize2(data);
    }

    public void flush(KeyValue props) throws IOException {

    }
}
