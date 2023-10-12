package com.hzx;

import com.hzx.common.CommonComsumerDataImpl;

public class ConsumerPipelineFactory {

    public static ConsumerData getConsumerData(String topic){
        switch (topic){
            default:
                return new CommonComsumerDataImpl();
        }

    }
}
