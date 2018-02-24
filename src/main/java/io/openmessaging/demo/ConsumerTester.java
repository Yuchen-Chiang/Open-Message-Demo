package io.openmessaging.demo;

import io.openmessaging.BytesMessage;
import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.MessageHeader;
import io.openmessaging.Producer;
import io.openmessaging.PullConsumer;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

public class ConsumerTester {

    static Logger logger = Logger.getLogger(ConsumerTester.class.getName());
    //0表示默认
    static AtomicInteger state = new AtomicInteger(0);
    static String errorMessage = "";

    static class ConsumerTask extends Thread {
        String queue;
        List<String> topics;
        PullConsumer consumer;
        int pullNum;
        Map<String, Map<String, Integer>> offsets = new HashMap<>();
        public ConsumerTask(String queue, List<String> topics) {
            this.queue = queue;
            this.topics = topics;
            init();
        }

        public void init() {
            //init consumer
            try {
                Class kvClass = Class.forName("io.openmessaging.demo.DefaultKeyValue");
                KeyValue keyValue = (KeyValue) kvClass.newInstance();
                keyValue.put("STORE_PATH", Constants.STORE_PATH);
                Class consumerClass = Class.forName("io.openmessaging.demo.DefaultPullConsumer");
                consumer = (PullConsumer) consumerClass.getConstructor(new Class[]{KeyValue.class}).newInstance(new Object[]{keyValue});
                if (consumer == null) {
                    throw new InstantiationException("Init Producer Failed");
                }
                consumer.attachQueue(queue, topics);
            } catch (Exception e) {
                logger.severe("please check the package name and class name:" + e);
            }
            //init offsets
            offsets.put(queue, new HashMap<>());
            for (String topic: topics) {
                offsets.put(topic, new HashMap<>());
            }
            for (Map<String, Integer> map: offsets.values()) {
                for (int i = 0; i < Constants.PRO_NUM; i++) {
                    map.put(Constants.PRO_PRE + i, 0);
                }
            }
        }

        @Override
        public void run() {
            while (true) {
                try {
                    BytesMessage message = (BytesMessage) consumer.poll();
                    if (message == null) {
                        break;
                    }
                    String queueOrTopic;
                    if (message.headers().getString(MessageHeader.QUEUE) != null) {
                        queueOrTopic = message.headers().getString(MessageHeader.QUEUE);
                    } else {
                        queueOrTopic = message.headers().getString(MessageHeader.TOPIC);
                    }
                    if (queueOrTopic == null || queueOrTopic.length() == 0) {
                        throw new Exception("Queue or Topic name is empty");
                    }
                    String body = new String(message.getBody());
                    int index = body.lastIndexOf("_");
                    String producer = body.substring(0, index);
                    int offset = Integer.parseInt(body.substring(index + 1));

                    if (offset != offsets.get(queueOrTopic).get(producer)) {
                        logger.severe("Offset not equal expected:" + offsets.get(producer) + "actual:" + offset + "producer:" + producer + "queueOrTopic:" + queueOrTopic);
                        break;
                    } else {
                        offsets.get(queueOrTopic).put(producer, offset + 1);
                    }
                    pullNum++;
                } catch (Exception e) {
                    logger.severe("Error occurred in the consuming process" + e);
                    break;
                }
            }
        }

        public int getPullNum() {
            return pullNum;
        }

    }

    public static void main(String[] args) throws Exception {
        Thread[] ts = new Thread[Constants.CON_NUM];
        for (int i = 0; i < ts.length; i++) {
//            List<String> topicList = new ArrayList<>();
//            topicList.add(Constants.TOPIC_PRE + i);
//
//            Random random = new Random();
//            for (int j = 0; j < 2; j++) {
//                int n = random.nextInt(10);
//                if (!topicList.contains(Constants.TOPIC_PRE + i)) {
//                    topicList.add(Constants.TOPIC_PRE + n);
//                }
//            }

            ts[i] = new ConsumerTask(Constants.QUEUE_PRE + i, Collections.singletonList(Constants.TOPIC_PRE + i));
            //ts[i] = new ConsumerTask(Constants.QUEUE_PRE + i, topicList);
        }
        long start = System.currentTimeMillis();
        for (int i = 0; i < ts.length; i++) {
            ts[i].start();
        }
        for (int i = 0; i < ts.length; i++) {
            ts[i].join();
        }
        int pullNum = 0;
        for (int i = 0; i < ts.length; i++) {
            pullNum += ((ConsumerTask)ts[i]).getPullNum();
        }
        long end = System.currentTimeMillis();
        logger.info("Consumer Finished, Cost " + (end - start) + " ms, Num " + pullNum);
    }
}
