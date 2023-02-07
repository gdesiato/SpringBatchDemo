package com.example.batch.demo.configuration;

import com.example.batch.demo.EmployeeRepository;
import com.example.batch.demo.model.Designation;
import com.example.batch.demo.model.Employee;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.data.RepositoryItemReader;
import org.springframework.batch.item.data.builder.RepositoryItemReaderBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Component;

import java.util.Date;
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

    @Bean
    public Step nameStep(StepBuilderFactory stepBuilderFactory, ItemReader<Employee> csvReader,
                         NameProcessor processor, EmployeeWriter writer) {
        // This step just reads the csv file and then writes the entries into the database
        return stepBuilderFactory.get("name-step")
                .<Employee, Employee>chunk(250)
                .reader(csvReader)      // EXTRACT
                .processor(processor)   // TRANSFORM
                .writer(writer)         // LOAD
                .build();
    }

    @Bean
    public Step designationStep(StepBuilderFactory stepBuilderFactory, ItemReader<Employee> repositoryReader,
                                DesignationProcessor processor, EmployeeWriter writer) {
        // This step reads the data from the database and then converts the designation into the matching Enums.
        return stepBuilderFactory.get("designation-step")
                .<Employee, Employee>chunk(250)
                .reader(repositoryReader)   // EXTRACT
                .processor(processor)       // TRANSFORM
                .writer(writer)             // LOAD
//                .faultTolerant()
//                .skipLimit(10)
//                .skip(Exception.class)
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

    @Component
    public static class NameProcessor implements ItemProcessor<Employee, Employee> {
        // This helps you to process the names of the employee at a set time
        @Override
        public Employee process(Employee employee) {
            employee.setName(employee.getName().toUpperCase());
            employee.setNameUpdatedAt(new Date());
            return employee;
        }
    }

    @Component
    public static class DesignationProcessor implements ItemProcessor<Employee, Employee> {
        // This helps you to convert the designations of the employees into the Enum you defined earlier
        @Override
        public Employee process(Employee employee) {
            employee.setDesignation(Designation.getByCode(employee.getDesignation()).getTitle());
            employee.setDesignationUpdatedAt(new Date());
            return employee;
        }
    }

    @Component
    public static class EmployeeWriter implements ItemWriter<Employee> {

        @Autowired
        private EmployeeRepository employeeRepository;

        @Value("${sleepTime}")
        private Integer SLEEP_TIME;

        @Override
        public void write(Chunk<? extends Employee> employees) throws Exception {
            employeeRepository.saveAll(employees);
            Thread.sleep(SLEEP_TIME);
            System.out.println("Saved employees: " + employees);
        }
    }
}