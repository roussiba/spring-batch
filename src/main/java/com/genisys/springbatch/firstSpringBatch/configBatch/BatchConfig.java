package com.genisys.springbatch.firstSpringBatch.configBatch;

import com.genisys.springbatch.firstSpringBatch.entity.Student;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.data.RepositoryItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@RequiredArgsConstructor
public class BatchConfig {

    private final StudentRepository repository;

    private final JobRepository jobRepository;

    private final PlatformTransactionManager platformTransactionManager;
    @Bean
    @StepScope
    public FlatFileItemReader<Student> itemReader(){
        FlatFileItemReader<Student> reader = new FlatFileItemReader<>();
        reader.setResource(new FileSystemResource("src/main/resources/customers.csv"));
        reader.setName("csvReader");
        reader.setLinesToSkip(1);
        reader.setLineMapper(lineMapper());

        return reader;
    }

    @Bean
    @StepScope
    public StudentProcessor studentProcessor(){
        return new StudentProcessor();
    }

    @Bean
    @StepScope
    public RepositoryItemWriter<Student> writer(){
        RepositoryItemWriter<Student> writer = new RepositoryItemWriter<>();
        writer.setRepository(repository);
        writer.setMethodName("save");
        return writer;
    }

    @Bean
    public Step importStep(){
        return new StepBuilder("importCsv", jobRepository)
                .<Student, Student>chunk(1, platformTransactionManager)
                .reader(itemReader())
                .processor(studentProcessor())
                .writer(writer())
                .build();
    }

    @Bean
    public Job runJobStudent(){
        return new JobBuilder("jobCsv", jobRepository)
                .start(importStep())
                .build();
    }

    public LineMapper<Student> lineMapper(){
        DefaultLineMapper<Student> lineMapper = new DefaultLineMapper<>();

        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setDelimiter(",");
        lineTokenizer.setStrict(false);
        lineTokenizer.setNames("id", "firstName", "lastName", "age");

        BeanWrapperFieldSetMapper<Student> fieldSet = new BeanWrapperFieldSetMapper<>();
        fieldSet.setTargetType(Student.class);

        lineMapper.setLineTokenizer(lineTokenizer);
        lineMapper.setFieldSetMapper(fieldSet);
        return lineMapper;
    }


}
