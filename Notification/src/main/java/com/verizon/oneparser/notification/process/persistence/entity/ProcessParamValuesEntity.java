package com.verizon.oneparser.notification.process.persistence.entity;

import org.hibernate.annotations.DynamicUpdate;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import java.sql.Timestamp;
import java.util.Objects;

@Entity
@DynamicUpdate
@Table(name = "process_param_values")
public class ProcessParamValuesEntity {
    private int id;
    private String paramCode;
    private String key;
    private String value;
    private String description;
    private Timestamp creationTime;
    private Timestamp dateModified;
    private String qcommStructChng;
    private String qcomm2StructChng;
    private String b192StructChng;
    private String b193StructChng;
    private String asnStructChng;
    private String nasStructChng;
    private String ipStructChng;
    private String qcomm5GStructChng;
    private String qcommCntChk;
    private String qcomm2CntChk;
    private String b192CntChk;
    private String b193CntChk;
    private String asnCntChk;
    private String nasCntChk;
    private String ipCntChk;
    private String qcomm5GCntChk;
    private String currentWeek;
    private String hiveStructChng;
    private String hiveCntChk;

    @Basic
    @Id
    @Column(name = "id")
    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    @Basic
    @Column(name = "param_code")
    public String getParamCode() {
        return paramCode;
    }

    public void setParamCode(String paramCode) {
        this.paramCode = paramCode;
    }

    @Basic
    @Column(name = "key")
    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    @Basic
    @Column(name = "value")
    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Basic
    @Column(name = "description")
    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    @Basic
    @Column(name = "creation_time")
    public Timestamp getCreationTime() {
        return creationTime;
    }

    public void setCreationTime(Timestamp creationTime) {
        this.creationTime = creationTime;
    }

    @Basic
    @Column(name = "date_modified")
    public Timestamp getDateModified() {
        return dateModified;
    }

    public void setDateModified(Timestamp dateModified) {
        this.dateModified = dateModified;
    }

    @Basic
    @Column(name = "qcomm_struct_chng")
    public String getQcommStructChng() {
        return qcommStructChng;
    }

    public void setQcommStructChng(String qcommStructChng) {
        this.qcommStructChng = qcommStructChng;
    }

    @Basic
    @Column(name = "qcomm2_struct_chng")
    public String getQcomm2StructChng() {
        return qcomm2StructChng;
    }

    public void setQcomm2StructChng(String qcomm2StructChng) {
        this.qcomm2StructChng = qcomm2StructChng;
    }

    @Basic
    @Column(name = "b192_struct_chng")
    public String getB192StructChng() {
        return b192StructChng;
    }

    public void setB192StructChng(String b192StructChng) {
        this.b192StructChng = b192StructChng;
    }

    @Basic
    @Column(name = "b193_struct_chng")
    public String getB193StructChng() {
        return b193StructChng;
    }

    public void setB193StructChng(String b193StructChng) {
        this.b193StructChng = b193StructChng;
    }

    @Basic
    @Column(name = "asn_struct_chng")
    public String getAsnStructChng() {
        return asnStructChng;
    }

    public void setAsnStructChng(String asnStructChng) {
        this.asnStructChng = asnStructChng;
    }

    @Basic
    @Column(name = "nas_struct_chng")
    public String getNasStructChng() {
        return nasStructChng;
    }

    public void setNasStructChng(String nasStructChng) {
        this.nasStructChng = nasStructChng;
    }

    @Basic
    @Column(name = "ip_struct_chng")
    public String getIpStructChng() {
        return ipStructChng;
    }

    public void setIpStructChng(String ipStructChng) {
        this.ipStructChng = ipStructChng;
    }

    @Basic
    @Column(name = "qcomm_5g_struct_chng")
    public String getQcomm5GStructChng() {
        return qcomm5GStructChng;
    }

    public void setQcomm5GStructChng(String qcomm5GStructChng) {
        this.qcomm5GStructChng = qcomm5GStructChng;
    }

    @Basic
    @Column(name = "qcomm_cnt_chk")
    public String getQcommCntChk() {
        return qcommCntChk;
    }

    public void setQcommCntChk(String qcommCntChk) {
        this.qcommCntChk = qcommCntChk;
    }

    @Basic
    @Column(name = "qcomm2_cnt_chk")
    public String getQcomm2CntChk() {
        return qcomm2CntChk;
    }

    public void setQcomm2CntChk(String qcomm2CntChk) {
        this.qcomm2CntChk = qcomm2CntChk;
    }

    @Basic
    @Column(name = "b192_cnt_chk")
    public String getB192CntChk() {
        return b192CntChk;
    }

    public void setB192CntChk(String b192CntChk) {
        this.b192CntChk = b192CntChk;
    }

    @Basic
    @Column(name = "b193_cnt_chk")
    public String getB193CntChk() {
        return b193CntChk;
    }

    public void setB193CntChk(String b193CntChk) {
        this.b193CntChk = b193CntChk;
    }

    @Basic
    @Column(name = "asn_cnt_chk")
    public String getAsnCntChk() {
        return asnCntChk;
    }

    public void setAsnCntChk(String asnCntChk) {
        this.asnCntChk = asnCntChk;
    }

    @Basic
    @Column(name = "nas_cnt_chk")
    public String getNasCntChk() {
        return nasCntChk;
    }

    public void setNasCntChk(String nasCntChk) {
        this.nasCntChk = nasCntChk;
    }

    @Basic
    @Column(name = "ip_cnt_chk")
    public String getIpCntChk() {
        return ipCntChk;
    }

    public void setIpCntChk(String ipCntChk) {
        this.ipCntChk = ipCntChk;
    }

    @Basic
    @Column(name = "qcomm_5g_cnt_chk")
    public String getQcomm5GCntChk() {
        return qcomm5GCntChk;
    }

    public void setQcomm5GCntChk(String qcomm5GCntChk) {
        this.qcomm5GCntChk = qcomm5GCntChk;
    }

    @Basic
    @Column(name = "current_week")
    public String getCurrentWeek() {
        return currentWeek;
    }

    public void setCurrentWeek(String currentWeek) {
        this.currentWeek = currentWeek;
    }

    @Basic
    @Column(name = "hive_struct_chng")
    public String getHiveStructChng() {
        return hiveStructChng;
    }

    public void setHiveStructChng(String hiveStructChng) {
        this.hiveStructChng = hiveStructChng;
    }

    @Basic
    @Column(name = "hive_cnt_chk")
    public String getHiveCntChk() {
        return hiveCntChk;
    }

    public void setHiveCntChk(String hiveCntChk) {
        this.hiveCntChk = hiveCntChk;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ProcessParamValuesEntity that = (ProcessParamValuesEntity) o;
        return id == that.id &&
                Objects.equals(paramCode, that.paramCode) &&
                Objects.equals(key, that.key) &&
                Objects.equals(value, that.value) &&
                Objects.equals(description, that.description) &&
                Objects.equals(creationTime, that.creationTime) &&
                Objects.equals(dateModified, that.dateModified) &&
                Objects.equals(qcommStructChng, that.qcommStructChng) &&
                Objects.equals(qcomm2StructChng, that.qcomm2StructChng) &&
                Objects.equals(b192StructChng, that.b192StructChng) &&
                Objects.equals(b193StructChng, that.b193StructChng) &&
                Objects.equals(asnStructChng, that.asnStructChng) &&
                Objects.equals(nasStructChng, that.nasStructChng) &&
                Objects.equals(ipStructChng, that.ipStructChng) &&
                Objects.equals(qcomm5GStructChng, that.qcomm5GStructChng) &&
                Objects.equals(qcommCntChk, that.qcommCntChk) &&
                Objects.equals(qcomm2CntChk, that.qcomm2CntChk) &&
                Objects.equals(b192CntChk, that.b192CntChk) &&
                Objects.equals(b193CntChk, that.b193CntChk) &&
                Objects.equals(asnCntChk, that.asnCntChk) &&
                Objects.equals(nasCntChk, that.nasCntChk) &&
                Objects.equals(ipCntChk, that.ipCntChk) &&
                Objects.equals(qcomm5GCntChk, that.qcomm5GCntChk) &&
                Objects.equals(currentWeek, that.currentWeek) &&
                Objects.equals(hiveStructChng, that.hiveStructChng) &&
                Objects.equals(hiveCntChk, that.hiveCntChk);
    }

    @Override
    public int hashCode() {
        return Objects
                .hash(id, paramCode, key, value, description, creationTime, dateModified, qcommStructChng,
                        qcomm2StructChng, b192StructChng, b193StructChng, asnStructChng, nasStructChng,
                        ipStructChng, qcomm5GStructChng, qcommCntChk, qcomm2CntChk, b192CntChk, b193CntChk,
                        asnCntChk, nasCntChk, ipCntChk, qcomm5GCntChk, currentWeek, hiveStructChng,
                        hiveCntChk);
    }
}
