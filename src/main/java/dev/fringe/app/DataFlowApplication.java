package dev.fringe.app;

import java.util.Set;
import java.util.UUID;

import javax.sql.DataSource;

import org.apache.commons.dbcp2.BasicDataSource;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.Banner;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.task.configuration.EnableTask;
import org.springframework.cloud.task.listener.annotation.AfterTask;
import org.springframework.cloud.task.listener.annotation.BeforeTask;
import org.springframework.cloud.task.repository.TaskExecution;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;
import org.springframework.context.annotation.PropertySource;

import lombok.extern.log4j.Log4j2;

@EnableTask
@Configuration
@PropertySource("classpath:app.properties")
@Log4j2
@ComponentScan("org.springframework.cloud.task.configuration")
@ImportResource("classpath:/app/job*.xml")
public class DataFlowApplication implements CommandLineRunner{

	
	@Autowired JobLauncher jobLauncher; @Autowired JobExplorer jobExplorer; @Autowired ApplicationContext context;
	
	public static void main(String[] args) {
		new SpringApplicationBuilder(DataFlowApplication.class).logStartupInfo(false).bannerMode(Banner.Mode.OFF).run(args);
	}

	@BeforeTask
	public void before(TaskExecution taskExecution) {
	}
	
	@AfterTask
	public void after(TaskExecution taskExecution) {
		taskExecution.setExitMessage("Custom Exit Message");
		taskExecution.setExitCode(1);
	}
	
	@EnableBatchProcessing
	public class BatchConfig{
		@Value("${app.jdbc.driver}") String driver;
		@Value("${app.jdbc.url}") String url;	
		@Value("${app.jdbc.user}") String user;	
		@Value("${app.jdbc.password}") String password;	
		
		@Bean
		public DataSource dataSource() {
			BasicDataSource ds = new BasicDataSource();
			ds.setDriverClassName(driver);
			ds.setUrl(url);
			ds.setUsername(user);
			ds.setPassword(password);
			return ds;
		}
	}


	@Override
	public void run(String... args) throws Exception {
		String jobName = "job"; 
		Set<JobExecution> jobExecutions = jobExplorer.findRunningJobExecutions(jobName);
		if(jobExecutions.size() > 0 ) {
			log.warn("jobName = "+ jobName +" is still running finish this job" );
			System.exit(1);
		}
		JobExecution execution = jobLauncher.run(context.getBean(jobName, Job.class), new JobParametersBuilder().addString("job",jobName).addString("uid", UUID.randomUUID().toString()).toJobParameters());
		if (execution.getStatus() != BatchStatus.COMPLETED) {
			log.info("finish - error");
			System.exit(1);
		}
		log.info("finish");
		
	}
}