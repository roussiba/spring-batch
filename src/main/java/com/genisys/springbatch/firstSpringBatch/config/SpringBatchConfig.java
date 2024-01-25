package com.genisys.springbatch.firstSpringBatch.config;

import com.genisys.springbatch.firstSpringBatch.entity.Customer;
import com.genisys.springbatch.firstSpringBatch.partition.ColumnRangePartitioner;
import com.genisys.springbatch.firstSpringBatch.repository.CustomerRepository;
import lombok.AllArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.partition.PartitionHandler;
import org.springframework.batch.core.partition.support.TaskExecutorPartitionHandler;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.data.RepositoryItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@AllArgsConstructor
public class SpringBatchConfig {

    private CustomerWriter customerWriter;

    @Bean
    @StepScope
    public FlatFileItemReader<Customer> reader(){
        FlatFileItemReader<Customer> itemReader = new FlatFileItemReader<>();
        itemReader.setResource(new FileSystemResource("src/main/resources/customers.csv"));
        itemReader.setName("csvReader");
        itemReader.setLinesToSkip(1);
        itemReader.setLineMapper(lineMapper());

        return itemReader;
    }

    private LineMapper<Customer> lineMapper(){
        DefaultLineMapper<Customer> lineMapper = new DefaultLineMapper<>();

        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setDelimiter(",");
        lineTokenizer.setStrict(false);
        lineTokenizer.setNames("id", "firstName", "lastName", "email", "gender", "contactNo", "country", "dob");

        BeanWrapperFieldSetMapper<Customer> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(Customer.class);

        lineMapper.setLineTokenizer(lineTokenizer);
        lineMapper.setFieldSetMapper(fieldSetMapper);

        return lineMapper;
    }

    @Bean
    @StepScope
    public CustomerProcessor processor(){
        return new CustomerProcessor();
    }

//    @Bean
//    public RepositoryItemWriter<Customer> writer(){
//        RepositoryItemWriter<Customer> writer = new RepositoryItemWriter<>();
//        writer.setRepository(customerRepository);
//        writer.setMethodName("save");
//        return writer;
//    }
    @Bean
    @StepScope
    public ColumnRangePartitioner partitioner(){
        return new ColumnRangePartitioner();
    }

    @Bean
    @StepScope
    public PartitionHandler partitionHandler(JobRepository jobRepository, PlatformTransactionManager transactionManager){
       TaskExecutorPartitionHandler partitionHandler = new TaskExecutorPartitionHandler();
       partitionHandler.setGridSize(2);
       partitionHandler.setTaskExecutor(taskExecutor());
       partitionHandler.setStep(slaveStep(jobRepository, transactionManager));
       return partitionHandler;
    }

    @Bean
    public Step slaveStep(JobRepository jobRepository, PlatformTransactionManager transactionalManager){
        return new StepBuilder("csv-step", jobRepository).<Customer, Customer>chunk(500,transactionalManager)
                .reader(reader())
                .processor(processor())
                .writer(customerWriter)
                .taskExecutor(taskExecutor())
                .build();
    }

    @Bean
    public Step masterStep(JobRepository jobRepository, PlatformTransactionManager transactionalManager){
        return new StepBuilder("masterStep", jobRepository)
                .partitioner(slaveStep(jobRepository, transactionalManager).getName(), partitioner())
                .partitionHandler(partitionHandler(jobRepository, transactionalManager))
                .build();
    }

    @Bean
    @Primary
    public Job runJobCustomer(JobRepository jobRepository, PlatformTransactionManager transactionManager){
        return new JobBuilder("importCustomer", jobRepository)
                .flow(masterStep(jobRepository, transactionManager))
                .end()
                .build();
    }

    @Bean
    public TaskExecutor taskExecutor(){
        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        taskExecutor.setMaxPoolSize(4);
        taskExecutor.setCorePoolSize(4);
        taskExecutor.setQueueCapacity(4);
        return taskExecutor;
    }
}
