package com.verizon.oneparser.notification.process.persistence.repository;

import com.verizon.oneparser.notification.process.persistence.entity.ProcessParamValuesEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import javax.transaction.Transactional;

public interface ProcessParamValuesRepository extends JpaRepository<ProcessParamValuesEntity, Integer> {
}
