package io.openmessaging;

/**
 * Created by Administrator on 2018-06-28.
 */
public class Msg {
    private String queueName;
    private byte[] message;

    public Msg(String queueName, byte[] message) {
        this.queueName = queueName;
        this.message = message;
    }

    public String getQueueName() {
        return queueName;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    public byte[] getMessage() {
        return message;
    }

    public void setMessage(byte[] message) {
        this.message = message;
    }
}
