package com.example.springbatchchunk.config;

import com.example.springbatchchunk.config.partitioner.SendStudentPartitioner;
import com.example.springbatchchunk.controller.StudentRowMapper;
import com.example.springbatchchunk.model.Student;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.configuration.support.DefaultBatchConfiguration;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.partition.support.SimplePartitioner;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.database.*;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.item.database.support.MySqlPagingQueryProvider;
import org.springframework.batch.item.database.support.SqlPagingQueryProviderFactoryBean;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.batch.BatchDataSourceScriptDatabaseInitializer;
import org.springframework.boot.autoconfigure.batch.BatchProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.jdbc.core.ArgumentPreparedStatementSetter;
import org.springframework.jdbc.core.ParameterizedPreparedStatementSetter;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.jdbc.core.SqlParameterValue;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterUtils;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.Transactional;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.IntStream;

@Configuration
@EnableConfigurationProperties(BatchProperties.class)
public class CustomBatchConfig extends DefaultBatchConfiguration {
    final BatchProperties batchProperties;
    final DataSource dataSourceOne;
    final DataSource dataSourceTwo;
    final DataSourceTransactionManager transactionManagerOne;
//    final JtaTransactionManager jtaTransactionManager;

    public CustomBatchConfig(BatchProperties batchProperties,
                             @Qualifier("dataSourceOne") DataSource dataSourceOne,
                             @Qualifier("dataSourceTwo") DataSource dataSourceTwo,
                             @Qualifier("transactionManagerOne") DataSourceTransactionManager transactionManagerOne
                             //     , JtaTransactionManager jtaTransactionManager
    ) {
        this.batchProperties = batchProperties;
        this.dataSourceOne = dataSourceOne;
        this.dataSourceTwo = dataSourceTwo;
        this.transactionManagerOne = transactionManagerOne;
        //   this.jtaTransactionManager = jtaTransactionManager;
    }


    @Override
    protected DataSource getDataSource() {
        return this.dataSourceOne;
    }

    @Override
    protected PlatformTransactionManager getTransactionManager() {
        return transactionManagerOne;
    }

    @Bean
    public BatchDataSourceScriptDatabaseInitializer batchDataSourceScriptDatabaseInitializer() {
        return new BatchDataSourceScriptDatabaseInitializer(getDataSource(), batchProperties.getJdbc());
    }

    @Bean
    public Job sampleJob(Step sampleStepWorker) {

        return new JobBuilder("sampleJob", jobRepository())
                .incrementer(new RunIdIncrementer())
                .start(sampleStepWorker)
                .build();

    }

    @Bean
    public SendStudentPartitioner sendStudentPartitioner() {

        var range = IntStream.rangeClosed(0, 9).boxed().toArray();
        return new SendStudentPartitioner
                .Builder("SEQ", Set.of(range))
                .build();
    }

    @Bean
    public TaskExecutor chunkTaskExecutor() {
        var threadPoolTaskExecutor = new ThreadPoolTaskExecutor();
        threadPoolTaskExecutor.setCorePoolSize(3);
        threadPoolTaskExecutor.setMaxPoolSize(3);
        return threadPoolTaskExecutor;
    }

    @Bean
    public Step sampleStepMaster(Step sampleStepWorker,
                                 SendStudentPartitioner sendStudentPartitioner,
                                 TaskExecutor chunkTaskExecutor) {

        return new StepBuilder("sampleStepMaster", jobRepository())
                .partitioner(sampleStepWorker.getName(), sendStudentPartitioner)
                .step(sampleStepWorker)
                .taskExecutor(chunkTaskExecutor)
                .build();
    }

    //        @Bean
//    public Step sampleStepWorker(JdbcPagingItemReader<Student> jdbcPagingItemReader, JdbcBatchItemWriter<Student> jdbcBatchItemWriter) {
//
//        return new StepBuilder("sampleStep", jobRepository())
//                .<Student, Student>chunk(2, transactionManagerOne)
//                .reader(jdbcPagingItemReader)
//                .writer(jdbcBatchItemWriter)
//                .build();
//    }
    @Bean
    @Transactional
    public Step sampleStepWorker(JdbcCursorItemReader<Student> jdbcCursorItemReader,
                                 JdbcPagingItemReader<Student> jdbcPagingItemReader,
                                 JdbcBatchItemWriter<Student> jdbcBatchItemWriter) {

        return new StepBuilder("sampleStep", jobRepository())
                .<Student, Student>chunk(1000, transactionManagerOne)
                .reader(jdbcPagingItemReader)
                .writer(jdbcBatchItemWriter)
                .build();
    }


    @Bean
    @StepScope
    public JdbcCursorItemReader<Student> jdbcCursorItemReader(@Value("#{stepExecutionContext}") Map<String, Object> stepExecutionContext) {
        var SEQ = stepExecutionContext.get("SEQ");
        MapSqlParameterSource parameterSource = new MapSqlParameterSource("SEQ", SEQ);
        //  var sql1="SELECT id,name,department,age From student where right(id,1)=:SEQ";
        var sql1 = "SELECT id,name,department,age From student";
        var substituteNamedParameters = NamedParameterUtils.substituteNamedParameters(sql1, new MapSqlParameterSource("SEQ", SEQ));

        return new JdbcCursorItemReaderBuilder<Student>()
                .name("jdbcCursorItemReader")
                .dataSource(dataSourceOne)
                // .sql("SELECT id,name,department,age From student where right(id,1)=:SEQ")
                //  .sql(substituteNamedParameters)
                .sql(sql1)
                .rowMapper(new StudentRowMapper())
                .fetchSize(1000)
                //  .queryArguments(new MapSqlParameterSource(Map.of("SEQ",seq)))
                //  .preparedStatementSetter(new ArgumentPreparedStatementSetter(new Object[]{SEQ}))
                //    .preparedStatementSetter(new ArgumentPreparedStatementSetter(NamedParameterUtils.buildValueArray(sql1,parameterSource.getValues())))

                .build();

    }


    @Bean
    @StepScope
    public JdbcPagingItemReader<Student> jdbcPagingItemReader() throws Exception {
     //   @Value("#{stepExecutionContext[SEQ]}") int seq
        //   var seq = stepExecutionContext.get("SEQ");


        return new JdbcPagingItemReaderBuilder<Student>()
                .name("jdbcPagingItemReader")
                .dataSource(dataSourceOne)
                .fetchSize(6)
                .pageSize(2)
                .rowMapper(new StudentRowMapper())
                .queryProvider(createQueryProvider())
          //      .parameterValues(Map.of("SEQ", seq))
                .build();
    }


    private MySqlPagingQueryProvider createQueryProvider() {
        MySqlPagingQueryProvider queryProvider = new MySqlPagingQueryProvider();
        queryProvider.setSelectClause("*");
        queryProvider.setFromClause("student");
      //  queryProvider.setWhereClause("right(id,1)=:SEQ");
        queryProvider.setSortKeys(Map.of("id", Order.ASCENDING));
        return queryProvider;
    }

    @Bean
    @StepScope
    public JdbcBatchItemWriter<Student> jdbcBatchItemWriter() {

        return new JdbcBatchItemWriterBuilder<Student>()
                .dataSource(dataSourceTwo)
                .sql("Insert into student (name,department,age) values(:name,:department,:age)")
                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
                .build();
    }


}
