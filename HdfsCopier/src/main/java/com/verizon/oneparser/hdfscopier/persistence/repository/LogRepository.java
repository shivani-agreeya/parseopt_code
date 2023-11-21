package com.verizon.oneparser.hdfscopier.persistence.repository;

import com.verizon.oneparser.hdfscopier.persistence.entity.Log;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface LogRepository extends JpaRepository<Log, Long> {

    List<Log> findByStatusAndFiletypeAndProcessingServerOrderByIdAsc(String status, String filetype, String processingServer, Pageable pageable);
}
