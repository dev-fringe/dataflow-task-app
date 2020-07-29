package dev.fringe.app.tasklet;

import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;

import lombok.extern.log4j.Log4j2;

@Log4j2
public class BatchTasklet implements Tasklet {
	public RepeatStatus execute(StepContribution contribution, ChunkContext context) throws Exception {
		log.info("Hello, world!");
		return RepeatStatus.FINISHED;
	}

}