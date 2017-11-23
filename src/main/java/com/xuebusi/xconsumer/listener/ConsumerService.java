package com.xuebusi.xconsumer.listener;

import com.xuebusi.xconsumer.service.TestService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.stereotype.Service;

//import com.zhaopin.pojo.TbPerson;
//import com.zhaopin.service.PersonService;

/**
 * 消息监听服务器类
 *  单条消息处理
 *  自动提交offset
 * Created by SYJ on 2017/3/21.
 */
@Service
public class ConsumerService implements MessageListener<Object, Object> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerService.class);
    //private static List<TbPerson> personList = new ArrayList<TbPerson>();
    private static final Integer INSERT_BATCH_COUNT = 50;

//    @Autowired
//    private PersonService personService;

    @Autowired
    private TestService testService;

    /**
     * 消息监听方法
     * @param record
     */
    @Override
    public void onMessage(ConsumerRecord<Object, Object> record) {

        LOGGER.info("Before receiving:" + record.toString());
        String value = (String) record.value();
        testService.test();
    }

    /**
     * 单个TbPerson入库
     * @param record
     */
    public void insert(ConsumerRecord<Integer, String> record){
//        String value = record.value();
//        TbPerson person = JSON.parseObject(value, TbPerson.class);
//        personService.insert(person);
//        System.out.println("Single data writing to the database:" + record);
    }

    /**
     * 批量TbPerson入库
     * @param record
     */
    public void insertBatch(ConsumerRecord<Integer, String> record){
//        String value = record.value();
//        TbPerson person = JSON.parseObject(value, TbPerson.class);
//        personList.add(person);
//        if (personList.size() == INSERT_BATCH_COUNT) {
//            personService.insertBatch(personList);
//            System.out.println("Batch data writing to the database:" + personList);
//            personList.clear();
//        }
    }

}