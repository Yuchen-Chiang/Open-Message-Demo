package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.PullConsumer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class DefaultPullConsumer implements PullConsumer {

    //private MessageStoreMMap messageStore = MessageStoreMMap.getInstance();
    private MessageStoreMMapSingle messageStore = MessageStoreMMapSingle.getInstance();
    //private MessageStoreSingle messageStore = MessageStoreSingle.getInstance();
    //private MessageStoreMMap2 messageStore = MessageStoreMMap2.getInstance();
    private KeyValue properties;
    private String queue;
    private Set<String> buckets = new HashSet<>();
    private List<String> bucketList = new ArrayList<>();

    private int lastIndex = 0;

    public DefaultPullConsumer(KeyValue properties) {
        this.properties = properties;
    }


    @Override public KeyValue properties() {
        return properties;
    }

    //change u07 delete synchronized
    @Override public Message poll() {
        if (buckets.size() == 0 || queue == null) {
            return null;
        }

        //Round Robin
        while (bucketList.size() > 0) {
            if (lastIndex >= bucketList.size()) {
                lastIndex = 0;
            }
            Message msg = null;
            try {
                msg = messageStore.pullMessage(bucketList.get(lastIndex), properties);
            } catch (IOException ioe) {
                ioe.printStackTrace();
            }
            if (msg == null) {
                bucketList.remove(lastIndex);
                continue;
            }
            lastIndex++;

            return msg;
        }

        return null;
    }

    @Override public Message poll(KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public void ack(String messageId) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public void ack(String messageId, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public synchronized void attachQueue(String queueName, Collection<String> topics) {
        if (queue != null && !queue.equals(queueName)) {
            throw new ClientOMSException("You have alreadly attached to a queue " + queue);
        }
        queue = queueName;
        buckets.add(queueName);
        buckets.addAll(topics);
        bucketList.clear();
        bucketList.addAll(buckets);
    }


}
