package com.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;

/**
 * Hello world!
 *
 */
public class App
{


    public static void main( String[] args )
    {

        KafkaService kf=new KafkaService();


        //send message
        for(int i=0;i<10;i++){
            kf.send(new CustomObject(String.valueOf(i),"ace",(23+i)));
        }



        //consume and process consumed message
        final Consumer<String, CustomObject> consumer =kf.consume("msg");

        final int giveUp = 100;   int noRecordsCount = 0;


        while (true) {
            final ConsumerRecords<String, CustomObject> consumerRecords =
                    consumer.poll(1000);
            if (consumerRecords.count()==0) {
                noRecordsCount++;
                if (noRecordsCount > giveUp) break;
                else continue;
            }
            consumerRecords.forEach(record -> {
                System.out.println(record.value());

            });
            consumer.commitAsync();
        }
        consumer.close();












    }
}
