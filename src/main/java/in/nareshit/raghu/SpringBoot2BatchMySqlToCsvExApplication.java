package in.nareshit.raghu;

import java.net.MalformedURLException;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class SpringBoot2BatchMySqlToCsvExApplication {

	public static void main(String[] args) throws MalformedURLException {
		SpringApplication.run(SpringBoot2BatchMySqlToCsvExApplication.class, args);
	}

}
