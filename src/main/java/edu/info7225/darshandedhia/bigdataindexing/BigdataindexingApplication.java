package edu.info7225.darshandedhia.bigdataindexing;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import edu.info7225.darshandedhia.bigdataindexing.Filter.*;
import edu.info7225.darshandedhia.bigdataindexing.service.QueueListenerService;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;


@SpringBootApplication
public class BigdataindexingApplication {

    public static void main(String[] args) throws InterruptedException{
            SpringApplication.run(BigdataindexingApplication.class, args);
    }
    
    @Bean
    public FilterRegistrationBean<AuthFilter> filterRegistrationBean() {


        System.out.println("APP CONFIG JAVA ");
        FilterRegistrationBean<AuthFilter> registrationBean = new FilterRegistrationBean();
        AuthFilter authFilter = new AuthFilter();

        registrationBean.setFilter(authFilter);
        registrationBean.addUrlPatterns("*");
        return registrationBean;
    }
    
    public final static String MESSAGE_QUEUE = "indexing-queue";

    @Bean
    Queue queue() {
        return new Queue(MESSAGE_QUEUE, false);
    }

    @Bean
    TopicExchange exchange() {
        return new TopicExchange("spring-boot-exchange");
    }

    @Bean
    Binding binding(Queue queue, TopicExchange exchange) {
        return BindingBuilder.bind(queue).to(exchange).with(MESSAGE_QUEUE);
    }

    @Bean
    SimpleMessageListenerContainer container(ConnectionFactory connectionFactory,
            MessageListenerAdapter listenerAdapter) {

        SimpleMessageListenerContainer container = new SimpleMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);
        container.setQueueNames(MESSAGE_QUEUE);
        container.setMessageListener(listenerAdapter);
        return container;
    }

    @Bean
    MessageListenerAdapter listenerAdapter(QueueListenerService receiver) {
        return new MessageListenerAdapter(receiver, "receiveMessage");
    }

}
