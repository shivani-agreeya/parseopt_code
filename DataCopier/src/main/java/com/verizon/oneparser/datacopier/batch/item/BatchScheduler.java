package com.verizon.oneparser.datacopier.batch.item;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Date;

@Slf4j
@Component
@EnableScheduling
@RequiredArgsConstructor
public class BatchScheduler {
    private final JobLauncher jobLauncher;
    private final Job logsJob;
    private boolean enabled = true;

    @Scheduled(fixedRateString = "${data.job.fixedRate:60000}")
    public void run() throws JobParametersInvalidException, JobExecutionAlreadyRunningException,
            JobRestartException, JobInstanceAlreadyCompleteException {
        if(enabled) {
            Date date = new Date();
            log.debug("scheduler starts at " + date);
            JobExecution jobExecution = jobLauncher
                    .run(logsJob, new JobParametersBuilder()
                            .addLong("uniqueness", System.currentTimeMillis())
                            .toJobParameters());
            log.debug("Batch job ends with status as " + jobExecution.getStatus());
            if (jobExecution.getStatus().equals(BatchStatus.FAILED)) {
                enabled = false;
            }

        }
    }
}
