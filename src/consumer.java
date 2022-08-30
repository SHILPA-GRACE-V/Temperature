import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.json.JSONObject;

import java.util.Arrays;
import java.util.Properties;
import java.sql.*;
import java.util.Scanner;

public class consumer {
    public static void main(String[] args) {
        KafkaConsumer consumer;
        Properties props=new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        props.put("group.id","test");
        consumer=new KafkaConsumer<>(props);
        KafkaConsumer obj=new KafkaConsumer(props);
        consumer.subscribe(Arrays.asList("nestDigital"));
        while(true){
            ConsumerRecords<String,String> records=consumer.poll(100);
            for(ConsumerRecord<String,String> record:records){
                System.out.println(record.value());

                String fetchedValue=record.value();
                JSONObject jsonObject=new JSONObject(fetchedValue);
                String tempValue=String.valueOf(jsonObject.getInt("temp"));
                System.out.println(tempValue);
                String humidityValue=String.valueOf(jsonObject.getInt("humidity"));
                System.out.println(humidityValue);


                Connection conn =null;
                Statement stmt =null;
                try {
                    Class.forName("com.mysql.cj.jdbc.Driver");
                    conn = (Connection) DriverManager.getConnection("jdbc:mysql://localhost:3306/iotdb","root","Sgv3#321");
                    System.out.println("connection successful");
                    stmt = (Statement) conn.createStatement();
                    String qry="insert into temp(Temperature,humidity) values("+tempValue+","+humidityValue+")";
                    System.out.println(qry);
                    stmt.executeUpdate(qry);
                    System.out.println("Successful");
                }
                catch(Exception ex){
                    System.out.println(ex);
                }
            }
        }

    }
}
