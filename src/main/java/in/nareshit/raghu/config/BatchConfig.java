package in.nareshit.raghu.config;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;

import in.nareshit.raghu.model.User;

@EnableBatchProcessing
@Configuration
public class BatchConfig {

	@Autowired
	private DataSource dataSource;

	//1. reader object
	@Bean
	public ItemReader<User> reader() {
		return new JdbcCursorItemReader<>() {{
			setDataSource(dataSource);
			setSql("SELECT USR_ID,USR_NAME,USR_TYPE FROM USERTAB");
			setRowMapper(
					(rs,num)-> new User(
							rs.getInt("USR_ID"), 
							rs.getString("USR_NAME"),
							rs.getString("USR_TYPE")
							)
					);
		}};
	}

	//2. processor object
	@Bean
	public ItemProcessor<User, User> processor() {
		return item->item;
	}

	//3. writer object
	@Bean
	public ItemWriter<User> writer() {
		return new FlatFileItemWriter<>() {{
			setResource(new FileSystemResource("E:/myouts/users.csv"));
			setLineAggregator(new DelimitedLineAggregator<>() {{
				setDelimiter(",");
				setFieldExtractor(new BeanWrapperFieldExtractor<User>() {{
					setNames(new String[] {"userId","userName","userType"});
				}});
			}});
		}};
	}

	//4. listener object
	@Bean
	public JobExecutionListener listener() {
		return new JobExecutionListener() {

			public void beforeJob(JobExecution je) {
				System.out.println("JOB STARTED WITH STATUS=>"+je.getStatus());
			}

			public void afterJob(JobExecution je) {
				System.out.println("JOB FINISHED WITH STATUS=>"+je.getStatus());

			}
		};
	}

	//5. Step builder factory
	@Autowired
	private StepBuilderFactory sf;

	//6. step object
	@Bean
	public Step stepA() {
		return sf.get("stepA") //name
				.<User,User>chunk(3)
				.reader(reader())
				.processor(processor())
				.writer(writer())
				.build()
				;
	}

	//7. job builder factory
	@Autowired
	private JobBuilderFactory jf;

	//8. job object
	@Bean
	public Job jobA() {
		return jf.get("jobA")
				.listener(listener())
				.incrementer(new RunIdIncrementer())
				.start(stepA())
				.build()
				;
	}
}
