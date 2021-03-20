package edu.info7225.darshandedhia.bigdataindexing;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import edu.info7225.darshandedhia.bigdataindexing.Filter.*;


@SpringBootApplication
public class BigdataindexingApplication {

	public static void main(String[] args) {
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

}
