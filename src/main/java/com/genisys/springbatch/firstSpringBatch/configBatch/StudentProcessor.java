package com.genisys.springbatch.firstSpringBatch.configBatch;

import com.genisys.springbatch.firstSpringBatch.entity.Student;
import org.springframework.batch.item.ItemProcessor;

public class StudentProcessor implements ItemProcessor<Student, Student> {
    @Override
    public Student process(Student item) throws Exception {
        return item;
    }
}
