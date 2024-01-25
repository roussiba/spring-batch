package com.genisys.springbatch.firstSpringBatch.config;

import com.genisys.springbatch.firstSpringBatch.entity.Customer;
import com.genisys.springbatch.firstSpringBatch.repository.CustomerRepository;
import lombok.AllArgsConstructor;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class CustomerWriter implements ItemWriter<Customer> {

    @Autowired
    CustomerRepository customerRepository;

    @Override
    public void write(Chunk<? extends Customer> chunk) throws Exception {
        customerRepository.saveAll(chunk.getItems());
    }
}
