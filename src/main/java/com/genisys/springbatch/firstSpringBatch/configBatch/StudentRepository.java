package com.genisys.springbatch.firstSpringBatch.configBatch;

import com.genisys.springbatch.firstSpringBatch.entity.Student;
import org.springframework.data.jpa.repository.JpaRepository;

public interface StudentRepository extends JpaRepository<Student, Integer> {
}
