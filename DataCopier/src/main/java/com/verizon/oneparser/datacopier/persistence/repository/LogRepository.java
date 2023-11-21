package com.verizon.oneparser.datacopier.persistence.repository;

import com.verizon.oneparser.datacopier.persistence.entity.Log;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.transaction.annotation.Transactional;

import java.util.Set;

import static org.springframework.transaction.annotation.Propagation.REQUIRES_NEW;

@Transactional(propagation=REQUIRES_NEW, rollbackFor=Exception.class)
// prevents chunk's transaction failure when one of the chunk items fails
public interface LogRepository extends JpaRepository<Log, Long> {

    Set<Log> findByStatusAndFiletypeOrderByIdAsc(String status, String fileType);
}
