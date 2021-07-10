package skhappydelivery;

import skhappydelivery.config.kafka.KafkaProcessor;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Service
public class PolicyHandler{
    @Autowired CustomerRepository customerRepository;

    @StreamListener(KafkaProcessor.INPUT)
    public void wheneverPayed_Status(@Payload Payed payed){

        if(!payed.validate()) return;

        System.out.println("\n\n##### listener Status : " + payed.toJson() + "\n\n");

        // Sample Logic //
        Customer customer = new Customer();
        customerRepository.save(customer);
            
    }
    @StreamListener(KafkaProcessor.INPUT)
    public void wheneverDeliveryStarted_Status(@Payload DeliveryStarted deliveryStarted){

        if(!deliveryStarted.validate()) return;

        System.out.println("\n\n##### listener Status : " + deliveryStarted.toJson() + "\n\n");

        // Sample Logic //
        Customer customer = new Customer();
        customerRepository.save(customer);
            
    }
    @StreamListener(KafkaProcessor.INPUT)
    public void wheneverDeliveryClosed_Status(@Payload DeliveryClosed deliveryClosed){

        if(!deliveryClosed.validate()) return;

        System.out.println("\n\n##### listener Status : " + deliveryClosed.toJson() + "\n\n");

        // Sample Logic //
        Customer customer = new Customer();
        customerRepository.save(customer);
            
    }
    @StreamListener(KafkaProcessor.INPUT)
    public void wheneverPayCancelled_Status(@Payload PayCancelled payCancelled){

        if(!payCancelled.validate()) return;

        System.out.println("\n\n##### listener Status : " + payCancelled.toJson() + "\n\n");

        // Sample Logic //
        Customer customer = new Customer();
        customerRepository.save(customer);
            
    }
    @StreamListener(KafkaProcessor.INPUT)
    public void wheneverStoreOrderCancelled_Status(@Payload StoreOrderCancelled storeOrderCancelled){

        if(!storeOrderCancelled.validate()) return;

        System.out.println("\n\n##### listener Status : " + storeOrderCancelled.toJson() + "\n\n");

        // Sample Logic //
        Customer customer = new Customer();
        customerRepository.save(customer);
            
    }
    @StreamListener(KafkaProcessor.INPUT)
    public void wheneverStoreOrderAccepted_Status(@Payload StoreOrderAccepted storeOrderAccepted){

        if(!storeOrderAccepted.validate()) return;

        System.out.println("\n\n##### listener Status : " + storeOrderAccepted.toJson() + "\n\n");

        // Sample Logic //
        Customer customer = new Customer();
        customerRepository.save(customer);
            
    }
    @StreamListener(KafkaProcessor.INPUT)
    public void wheneverStoreOrderRejected_Status(@Payload StoreOrderRejected storeOrderRejected){

        if(!storeOrderRejected.validate()) return;

        System.out.println("\n\n##### listener Status : " + storeOrderRejected.toJson() + "\n\n");

        // Sample Logic //
        Customer customer = new Customer();
        customerRepository.save(customer);
            
    }


    @StreamListener(KafkaProcessor.INPUT)
    public void whatever(@Payload String eventString){}


}
