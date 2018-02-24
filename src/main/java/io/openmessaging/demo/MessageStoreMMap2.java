package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Yuchen Chiang
 */
public class MessageStoreMMap2 {

    private static final MessageStoreMMap2 INSTANCE = new MessageStoreMMap2();

    public static MessageStoreMMap2 getInstance() {
        return INSTANCE;
    }

    private static Lock lock = new ReentrantLock();

    private static final int LOG_FILE_MAX = 16 * 1024 * 1024;

    private static Map<String, List<String>> tmpMap = new HashMap<>();

    private ThreadLocal<Map<String, MappedByteBuffer>> bufLocW = new ThreadLocal<Map<String, MappedByteBuffer>>(){
        @Override
        protected Map<String, MappedByteBuffer> initialValue() {return new HashMap<>();
        }
    };

    private ThreadLocal<Map<String, ByteBuffer>> bufLocR = new ThreadLocal<Map<String, ByteBuffer>>(){
        @Override
        protected Map<String, ByteBuffer> initialValue() {
            return new HashMap<>();
        }
    };

    private ThreadLocal<Map<String, Integer>> tbID = new ThreadLocal<Map<String, Integer>>(){
        @Override
        protected Map<String, Integer> initialValue() {
            return new HashMap<>();
        }
    };

    private ThreadLocal<Map<String, Integer>> tbfID = new ThreadLocal<Map<String, Integer>>() {
        @Override
        protected Map<String, Integer> initialValue() {
            return new HashMap<>();
        }
    };

    public void putMessage(String bucket, Message msg, KeyValue props) throws IOException {

        String storePath = props.getString("STORE_PATH");

        Map<String, MappedByteBuffer> bufMap = bufLocW.get();
        MappedByteBuffer buf;

        Map<String, Integer> tbIDMap = tbID.get();
        if (!tbIDMap.containsKey(bucket)) {
            tbIDMap.put(bucket, 0);
        }
        int id = tbIDMap.get(bucket);

        if (!bufMap.containsKey(bucket)) {

            if (!Files.exists(Paths.get(storePath + File.separator + bucket))) {
                synchronized (MessageStoreMMap.class) {
                    if (!Files.exists(Paths.get(storePath + File.separator + bucket))) {
                        Files.createDirectories(Paths.get(storePath + File.separator + bucket));
                    }
                }
            }

            buf = new RandomAccessFile(storePath + File.separator + bucket + File.separator + Thread.currentThread() + String.format("%05d", id++) + ".log", "rw").getChannel().map(FileChannel.MapMode.READ_WRITE, 0, LOG_FILE_MAX);
            bufMap.put(bucket, buf);
            tbIDMap.put(bucket, id);
        }

        buf = bufMap.get(bucket);

        byte[] data = MsgSerializer.serialize2(msg);
        buf.put(data);
    }

    public Message pullMessage(String bucket, KeyValue props) throws IOException {

        String storePath = props.getString("STORE_PATH");

        if (tmpMap.isEmpty()) {
            synchronized (MessageStoreMMap.class) {
                if (tmpMap.isEmpty()) {
                    tmpMap = CacheFileLoader.load(storePath);
                }
            }
        }

        Map<String, Integer> tbfIDMap = tbfID.get();

        Map<String, ByteBuffer> bufMap = bufLocR.get();
        ByteBuffer buf;
        FileChannel fileChannel = null;

        if (!bufMap.containsKey(bucket)) {

            List<String> list = tmpMap.get(bucket);
            if (list == null) {
                return null;
            }

            if (!tbfIDMap.containsKey(bucket)) {
                tbfIDMap.put(bucket, 0);
            }
            int id = tbfIDMap.get(bucket);

            fileChannel = new RandomAccessFile(list.get(id), "r").getChannel();
            buf = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());
            bufMap.put(bucket, buf);
            tbfIDMap.put(bucket, ++id);
        }

        buf = bufMap.get(bucket);

        int length;
        while ((length = buf.getInt()) == 0) {
            int id = tbfIDMap.get(bucket);
            List<String> list = tmpMap.get(bucket);
            if (id >= list.size()) {
                return null;
            }

            fileChannel = new RandomAccessFile(list.get(id), "r").getChannel();
            buf = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());
            bufMap.put(bucket, buf);
            tbfIDMap.put(bucket, ++id);
        }

        byte[] data = new byte[length];
        buf.get(data);

        return MsgSerializer.deserialize2(data);
    }

    public void flush(KeyValue props) throws IOException {

    }
}