package com.example.springbatchchunk.controller;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Random;

@RestController
public class StudentController {

    final private JobLauncher jobLauncher;
    final private Job sampleJob;

    public StudentController(JobLauncher jobLauncher, @Qualifier("sampleJob") Job sampleJob) {
        this.jobLauncher = jobLauncher;
        this.sampleJob = sampleJob;
    }

    @GetMapping("/start")
    public String startJob() throws JobInstanceAlreadyCompleteException, JobExecutionAlreadyRunningException, JobParametersInvalidException, JobRestartException {
        var jobParameters = new JobParametersBuilder()
                .addLong("Random"+new Random().nextLong(100000000000000000L), new Random().nextLong(100000000000000000L))
                .toJobParameters();
        var jobExecution = jobLauncher.run(sampleJob, jobParameters);

        return jobExecution.getExitStatus().toString();
    }
}
