package com.example.batch.demo.configuration;

import com.example.batch.demo.EmployeeRepository;
import com.example.batch.demo.model.Employee;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.data.RepositoryItemReader;
import org.springframework.batch.item.data.builder.RepositoryItemReaderBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.domain.Sort;

import java.util.Map;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

    @Bean
    public Job job(JobBuilderFactory jobBuilderFactory, Step nameStep, Step designationStep) {
        return jobBuilderFactory.get("employee-loader-job")
                .incrementer(new RunIdIncrementer())
                .start(nameStep)
                .next(designationStep)
                .build();
    }

    //add your Steps, ItemReaders, ItemProcessors, and ItemWriter below

    @Bean
    public Step nameStep(StepBuilderFactory stepBuilderFactory, ItemReader<Employee> csvReader, NameProcessor processor, EmployeeWriter writer) {
        // This step just reads the csv file and then writes the entries into the database
        return stepBuilderFactory.get("name-step")
                .<Employee, Employee>chunk(100)
                .reader(csvReader)
                .processor(processor)
                .writer(writer)
                .allowStartIfComplete(false)
                .build();
    }

    @Bean
    public Step designationStep(StepBuilderFactory stepBuilderFactory, ItemReader<Employee> repositoryReader, DesignationProcessor processor, EmployeeWriter writer) {
        // This step reads the data from the database and then converts the designation into the matching Enums.
        return stepBuilderFactory.get("designation-step")
                .<Employee, Employee>chunk(100)
                .reader(repositoryReader)
                .processor(processor)
                .writer(writer)
                .build();
    }

    @Bean
    public FlatFileItemReader<Employee> csvReader(@Value("${inputFile}") String inputFile) {
        return new FlatFileItemReaderBuilder<Employee>()
                .name("csv-reader")
                .resource(new ClassPathResource(inputFile))
                .delimited()
                .names("id", "name", "designation")
                .linesToSkip(1)
                .fieldSetMapper(new BeanWrapperFieldSetMapper<>() {{setTargetType(Employee.class);}})
                .build();
    }

    @Bean
    public RepositoryItemReader<Employee> repositoryReader(EmployeeRepository employeeRepository) {
        return new RepositoryItemReaderBuilder<Employee>()
                .repository(employeeRepository)
                .methodName("findAll")
                .sorts(Map.of("id", Sort.Direction.ASC))
                .name("repository-reader")
                .build();
    }

    
}