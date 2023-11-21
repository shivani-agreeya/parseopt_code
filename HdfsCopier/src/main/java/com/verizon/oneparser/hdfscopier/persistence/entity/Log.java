package com.verizon.oneparser.hdfscopier.persistence.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import java.sql.Timestamp;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Entity
@Table(name = "logs")
public class Log {

    @Id
    @GeneratedValue(strategy=GenerationType.IDENTITY)
    private Long id;
//    @Column(nullable = false)
    private Integer dmUser;
    @CreationTimestamp
//    @Column(nullable = false)
    private Timestamp creationTime;
    private String fileName;
    private Double size; //numeric
    private String status;
    private String fileLocation;
    private String esDataStatus;
    private String filetype;
    @UpdateTimestamp
    private Timestamp updatedTime;
    @UpdateTimestamp
    private Timestamp cpHdfsTime;
    private String failureInfo;
    private String lvPick;
    private String processingServer;
    private Boolean compressionstatus;
}
