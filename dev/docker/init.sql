CREATE EXTENSION postgis;
create role sde;
create schema sde;
create role dmat_qa;
create schema dmat_qa;

CREATE TABLE sde.event_lvanalysis (
                                      objectid integer NOT NULL,
                                      filename text,
                                      testid integer,
                                      evt_timestamp timestamp without time zone,
                                      lvfid integer,
                                      lat numeric(40,8),
                                      lon numeric(40,8),
                                      user_id integer,
                                      lteattrej integer DEFAULT 0,
                                      ltetarej integer DEFAULT 0,
                                      lteservicerej integer DEFAULT 0,
                                      lteauthrej integer DEFAULT 0,
                                      ltepdnrej integer DEFAULT 0,
                                      lterrcconrej integer DEFAULT 0,
                                      lterrcconrestrej integer DEFAULT 0,
                                      lteintrahofail integer DEFAULT 0,
                                      lteintrareselfail integer DEFAULT 0,
                                      ltemobilityfromeutrafail integer DEFAULT 0,
                                      ltereseltogsmumtsfail integer DEFAULT 0,
                                      ltesibreadfailure integer DEFAULT 0,
                                      ltereselfromgsmumtsfail integer DEFAULT 0,
                                      lterlf integer DEFAULT 0,
                                      lteoos integer DEFAULT 0,
                                      ltevoltedrop integer DEFAULT 0,
                                      shape bytea,
                                      shape_geo public.geometry,
                                      file_path character varying(2000),
                                      lva_updated character varying(4),
                                      creation_time timestamp without time zone,
                                      status character varying DEFAULT 'NEW'::character varying,
                                      ltevolteabort integer DEFAULT 0,
                                      logevfilestatusforlv text,
                                      lvevupdatedtime timestamp without time zone,
                                      lveverrordetails text,
                                      lvevfilestarttime timestamp without time zone,
                                      lvevparseduserid character varying(255),
                                      lvevlogfilecnt bigint,
                                      lvevlogparsedinterval bigint,
                                      nrrlf integer DEFAULT 0,
                                      ltecallinefatmpt integer DEFAULT 0
);

ALTER TABLE sde.event_lvanalysis OWNER TO sde;

ALTER TABLE ONLY sde.event_lvanalysis
    ADD CONSTRAINT "PK2_ObjectId" PRIMARY KEY (objectid);

CREATE INDEX idx_event_lvanalysis_timestamp ON sde.event_lvanalysis USING btree (evt_timestamp);


CREATE SEQUENCE sde.lvsequence
    START WITH 302195
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER TABLE sde.lvsequence OWNER TO postgres;


CREATE TABLE dmat_qa.logs (
                              dm_user integer NOT NULL,
                              creation_time timestamp without time zone NOT NULL,
                              last_log_time timestamp without time zone,
                              file_name text,
                              size numeric(18,2),
                              mdn character varying(45),
                              imei character varying(45),
                              id bigserial NOT NULL,
                              firstname text,
                              lastname text,
                              isinbuilding integer,
                              hasgps integer DEFAULT 0 NOT NULL,
                              emailid character varying(255),
                              status character varying(50),
                              model_name character varying(20),
                              file_location character varying(2000),
                              es_data_status character varying(50),
                              es_record_count numeric(10,0),
                              importstarttime timestamp without time zone DEFAULT now(),
                              importstoptime timestamp without time zone DEFAULT now(),
                              filetype text,
                              start_log_time timestamp without time zone,
                              updated_time timestamp without time zone DEFAULT now(),
                              failure_info text,
                              logfilestatusforlv text,
                              lvupdatedtime timestamp without time zone,
                              lverrordetails text,
                              status_csv_files text,
                              csv_updated_time timestamp without time zone,
                              csv_error_details text,
                              technology character varying DEFAULT '4g'::character varying NOT NULL,
                              seq_start_time timestamp without time zone,
                              seq_end_time timestamp without time zone,
                              hive_start_time timestamp without time zone,
                              hive_end_time timestamp without time zone,
                              es_start_time timestamp without time zone,
                              es_end_time timestamp without time zone,
                              lvfilestarttime timestamp without time zone,
                              missing_versions text,
                              lv_pick character varying,
                              lv_status character varying(50),
                              lvparseduserid character varying(255),
                              lvlogfilecnt bigint,
                              processing_server character varying(15) DEFAULT 'DEV'::character varying,
                              reports_hive_tbl_name character varying(30),
                              compressionstatus boolean DEFAULT false,
                              events_count integer DEFAULT 0,
                              ftpstatus boolean DEFAULT true,
                              user_priority_time timestamp without time zone,
                              reprocessed_by integer,
                              pcap_multi character varying(25) DEFAULT 'READY'::character varying,
                              pcap_single_rm character varying(25) DEFAULT 'READY'::character varying,
                              pcap_single_um character varying(25) DEFAULT 'READY'::character varying,
                              cp_hdfs_time timestamp without time zone,
                              cp_hdfs_start_time timestamp without time zone,
                              cp_hdfs_end_time timestamp without time zone,
                              hive_dir_epoch character varying(100) DEFAULT NULL::character varying,
                              ondemand_by integer,
                              carrier_name character varying(50) DEFAULT 'Verizon'::character varying,
                              odlv_upload boolean NULL DEFAULT false,
                              odlv_user_id integer NULL,
                              odlv_onp_status character varying(50) NULL DEFAULT NULL::character varying,
                              odlv_lv_status character varying(50) NULL DEFAULT NULL::character varying,
                              zip_start_time timestamp without time zone NULL,
                              zip_end_time timestamp without time zone NULL
);

ALTER TABLE dmat_qa.logs OWNER TO postgres;

ALTER TABLE ONLY dmat_qa.logs
    ADD CONSTRAINT logs_pkey PRIMARY KEY (id);

CREATE INDEX logs_creation_idx ON dmat_qa.logs USING btree (creation_time);

CREATE INDEX logs_dm_user_idx ON dmat_qa.logs USING btree (dm_user) WITH (fillfactor='70');

CREATE INDEX logs_filename_idx ON dmat_qa.logs USING btree (file_name);

CREATE INDEX logs_lastlogtime_idx ON dmat_qa.logs USING btree (last_log_time);

CREATE TABLE dmat_qa.process_param_values (
                                              id integer NOT NULL,
                                              param_code character varying(32),
                                              key character varying(32),
                                              value character varying(64),
                                              description character varying(64),
                                              creation_time timestamp(6) without time zone DEFAULT now(),
                                              date_modified timestamp(6) without time zone DEFAULT now(),
                                              current_week character varying,
                                              hive_struct_chng character varying(5),
                                              hive_cnt_chk character varying(5),
                                              qcomm_cnt_chk character varying(5) DEFAULT 'N'::character varying,
                                              qcomm2_cnt_chk character varying(5) DEFAULT 'N'::character varying,
                                              qcomm_5g_cnt_chk character varying(5) DEFAULT 'N'::character varying,
                                              b192_cnt_chk character varying(5) DEFAULT 'N'::character varying,
                                              b193_cnt_chk character varying(5) DEFAULT 'N'::character varying,
                                              asn_cnt_chk character varying(5) DEFAULT 'N'::character varying,
                                              nas_cnt_chk character varying(5) DEFAULT 'N'::character varying,
                                              ip_cnt_chk character varying(5) DEFAULT 'N'::character varying,
                                              qcomm_struct_chng character varying(5) DEFAULT 'N'::character varying,
                                              qcomm2_struct_chng character varying(5) DEFAULT 'N'::character varying,
                                              qcomm_5g_struct_chng character varying(5) DEFAULT 'N'::character varying,
                                              b192_struct_chng character varying(5) DEFAULT 'N'::character varying,
                                              b193_struct_chng character varying(5) DEFAULT 'N'::character varying,
                                              asn_struct_chng character varying(5) DEFAULT 'N'::character varying,
                                              nas_struct_chng character varying(5) DEFAULT 'N'::character varying,
                                              ip_struct_chng character varying(5) DEFAULT 'N'::character varying
);

ALTER TABLE dmat_qa.process_param_values OWNER TO postgres;

CREATE SEQUENCE dmat_qa.process_param_values_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER TABLE dmat_qa.process_param_values_id_seq OWNER TO postgres;

ALTER SEQUENCE dmat_qa.process_param_values_id_seq OWNED BY dmat_qa.process_param_values.id;

ALTER TABLE ONLY dmat_qa.process_param_values ALTER COLUMN id SET DEFAULT nextval('dmat_qa.process_param_values_id_seq'::regclass);

ALTER TABLE ONLY dmat_qa.process_param_values
    ADD CONSTRAINT pcv_pkey PRIMARY KEY (id);

GRANT ALL ON TABLE dmat_qa.process_param_values TO postgres;
GRANT ALL ON TABLE dmat_qa.process_param_values TO sde;
GRANT ALL ON TABLE dmat_qa.process_param_values TO dmat_qa;

INSERT INTO dmat_qa.process_param_values (id, param_code, key, value, description, creation_time, date_modified, qcomm_struct_chng, qcomm2_struct_chng, b192_struct_chng, b193_struct_chng, asn_struct_chng, nas_struct_chng, ip_struct_chng, qcomm_5g_struct_chng, qcomm_cnt_chk, qcomm2_cnt_chk, b192_cnt_chk, b193_cnt_chk, asn_cnt_chk, nas_cnt_chk, ip_cnt_chk, qcomm_5g_cnt_chk, current_week, hive_struct_chng, hive_cnt_chk) VALUES (33, 'ONE_PARSER', 'DEV', 'Y', 'Parameter to control file processing', '2019-06-06 10:20:57.524205', '2020-02-14 04:07:26.351553', 'Y', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'Y', 'N', 'N', 'N', 'N', 'N', 'N', 'N', '_2020_07Part2', 'N', 'N');
INSERT INTO dmat_qa.process_param_values (id, param_code, key, value, description, creation_time, date_modified, qcomm_struct_chng, qcomm2_struct_chng, b192_struct_chng, b193_struct_chng, asn_struct_chng, nas_struct_chng, ip_struct_chng, qcomm_5g_struct_chng, qcomm_cnt_chk, qcomm2_cnt_chk, b192_cnt_chk, b193_cnt_chk, asn_cnt_chk, nas_cnt_chk, ip_cnt_chk, qcomm_5g_cnt_chk, current_week, hive_struct_chng, hive_cnt_chk) VALUES (32, 'PROCESS_FILES', '192.168.1.11', 'Y', 'Parameter to control file processing', '2019-01-24 14:33:20.696753', '2019-01-24 14:33:20.696753', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', NULL, NULL, NULL);
INSERT INTO dmat_qa.process_param_values (id, param_code, key, value, description, creation_time, date_modified, qcomm_struct_chng, qcomm2_struct_chng, b192_struct_chng, b193_struct_chng, asn_struct_chng, nas_struct_chng, ip_struct_chng, qcomm_5g_struct_chng, qcomm_cnt_chk, qcomm2_cnt_chk, b192_cnt_chk, b193_cnt_chk, asn_cnt_chk, nas_cnt_chk, ip_cnt_chk, qcomm_5g_cnt_chk, current_week, hive_struct_chng, hive_cnt_chk) VALUES (26, 'PROCESS_FILES', '192.168.1.11', 'N', 'Parameter to control file processing', '2018-10-10 15:56:47.656489', '2018-10-10 15:56:47.656489', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', NULL, NULL, NULL);
INSERT INTO dmat_qa.process_param_values (id, param_code, key, value, description, creation_time, date_modified, qcomm_struct_chng, qcomm2_struct_chng, b192_struct_chng, b193_struct_chng, asn_struct_chng, nas_struct_chng, ip_struct_chng, qcomm_5g_struct_chng, qcomm_cnt_chk, qcomm2_cnt_chk, b192_cnt_chk, b193_cnt_chk, asn_cnt_chk, nas_cnt_chk, ip_cnt_chk, qcomm_5g_cnt_chk, current_week, hive_struct_chng, hive_cnt_chk) VALUES (27, 'PROCESS_FILES', '10.0.0.36', 'N', 'Parameter to control file processing', '2018-10-11 14:00:56.648982', '2018-10-11 14:00:56.648982', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', NULL, NULL, NULL);
INSERT INTO dmat_qa.process_param_values (id, param_code, key, value, description, creation_time, date_modified, qcomm_struct_chng, qcomm2_struct_chng, b192_struct_chng, b193_struct_chng, asn_struct_chng, nas_struct_chng, ip_struct_chng, qcomm_5g_struct_chng, qcomm_cnt_chk, qcomm2_cnt_chk, b192_cnt_chk, b193_cnt_chk, asn_cnt_chk, nas_cnt_chk, ip_cnt_chk, qcomm_5g_cnt_chk, current_week, hive_struct_chng, hive_cnt_chk) VALUES (28, 'PROCESS_FILES', '192.168.1.32', 'N', 'Parameter to control file processing', '2018-11-06 16:02:27.793659', '2018-11-06 16:02:27.793659', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', NULL, NULL, NULL);
INSERT INTO dmat_qa.process_param_values (id, param_code, key, value, description, creation_time, date_modified, qcomm_struct_chng, qcomm2_struct_chng, b192_struct_chng, b193_struct_chng, asn_struct_chng, nas_struct_chng, ip_struct_chng, qcomm_5g_struct_chng, qcomm_cnt_chk, qcomm2_cnt_chk, b192_cnt_chk, b193_cnt_chk, asn_cnt_chk, nas_cnt_chk, ip_cnt_chk, qcomm_5g_cnt_chk, current_week, hive_struct_chng, hive_cnt_chk) VALUES (29, 'PROCESS_FILES', '192.168.1.15', 'N', 'Parameter to control file processing', '2018-12-14 14:51:08.132782', '2018-12-14 14:51:08.132782', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', NULL, NULL, NULL);
INSERT INTO dmat_qa.process_param_values (id, param_code, key, value, description, creation_time, date_modified, qcomm_struct_chng, qcomm2_struct_chng, b192_struct_chng, b193_struct_chng, asn_struct_chng, nas_struct_chng, ip_struct_chng, qcomm_5g_struct_chng, qcomm_cnt_chk, qcomm2_cnt_chk, b192_cnt_chk, b193_cnt_chk, asn_cnt_chk, nas_cnt_chk, ip_cnt_chk, qcomm_5g_cnt_chk, current_week, hive_struct_chng, hive_cnt_chk) VALUES (35, 'PROCESS_FILES', '10.20.40.241', 'Y', 'Parameter to control file processing', '2019-07-01 11:50:47.390128', '2019-07-01 11:50:47.390128', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', NULL, NULL, NULL);
INSERT INTO dmat_qa.process_param_values (id, param_code, key, value, description, creation_time, date_modified, qcomm_struct_chng, qcomm2_struct_chng, b192_struct_chng, b193_struct_chng, asn_struct_chng, nas_struct_chng, ip_struct_chng, qcomm_5g_struct_chng, qcomm_cnt_chk, qcomm2_cnt_chk, b192_cnt_chk, b193_cnt_chk, asn_cnt_chk, nas_cnt_chk, ip_cnt_chk, qcomm_5g_cnt_chk, current_week, hive_struct_chng, hive_cnt_chk) VALUES (1, 'PROCESS_FILES', '10.20.40.241', 'Y', 'Parameter to control file processing', '2018-02-12 11:00:29.297258', '2018-02-27 10:12:33.571758', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', NULL, NULL, NULL);
INSERT INTO dmat_qa.process_param_values (id, param_code, key, value, description, creation_time, date_modified, qcomm_struct_chng, qcomm2_struct_chng, b192_struct_chng, b193_struct_chng, asn_struct_chng, nas_struct_chng, ip_struct_chng, qcomm_5g_struct_chng, qcomm_cnt_chk, qcomm2_cnt_chk, b192_cnt_chk, b193_cnt_chk, asn_cnt_chk, nas_cnt_chk, ip_cnt_chk, qcomm_5g_cnt_chk, current_week, hive_struct_chng, hive_cnt_chk) VALUES (36, 'PROCESS_FILES', '192.168.1.11', 'Y', 'Parameter to control file processing', '2019-07-01 11:50:47.457967', '2019-07-01 11:50:47.457967', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', NULL, NULL, NULL);
INSERT INTO dmat_qa.process_param_values (id, param_code, key, value, description, creation_time, date_modified, qcomm_struct_chng, qcomm2_struct_chng, b192_struct_chng, b193_struct_chng, asn_struct_chng, nas_struct_chng, ip_struct_chng, qcomm_5g_struct_chng, qcomm_cnt_chk, qcomm2_cnt_chk, b192_cnt_chk, b193_cnt_chk, asn_cnt_chk, nas_cnt_chk, ip_cnt_chk, qcomm_5g_cnt_chk, current_week, hive_struct_chng, hive_cnt_chk) VALUES (37, 'PROCESS_FILES', '192.168.1.32', 'Y', 'Parameter to control file processing', '2019-07-01 11:50:47.466142', '2019-07-01 11:50:47.466142', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', NULL, NULL, NULL);
INSERT INTO dmat_qa.process_param_values (id, param_code, key, value, description, creation_time, date_modified, qcomm_struct_chng, qcomm2_struct_chng, b192_struct_chng, b193_struct_chng, asn_struct_chng, nas_struct_chng, ip_struct_chng, qcomm_5g_struct_chng, qcomm_cnt_chk, qcomm2_cnt_chk, b192_cnt_chk, b193_cnt_chk, asn_cnt_chk, nas_cnt_chk, ip_cnt_chk, qcomm_5g_cnt_chk, current_week, hive_struct_chng, hive_cnt_chk) VALUES (34, 'ONE_PARSER', 'DEV_BACKUP', 'Y', 'Parameter to control file processing', '2019-06-18 15:20:12.777078', '2019-06-19 11:39:02.008249', 'N', 'N', 'N', 'N', 'Y', 'Y', 'N', 'N', 'N', 'N', 'N', 'N', 'Y', 'Y', 'N', 'Y', NULL, NULL, NULL);

SELECT pg_catalog.setval('dmat_qa.process_param_values_id_seq', 37, true);

CREATE TABLE dmat_qa.users (
                               user_id integer NOT NULL,
                               first_name character varying(100) NOT NULL,
                               last_name character varying(100) NOT NULL,
                               email character varying(355) NOT NULL,
                               role_id integer,
                               last_login timestamp without time zone,
                               created_on date DEFAULT ('now'::text)::date NOT NULL
);


ALTER TABLE dmat_qa.users OWNER TO postgres;

CREATE SEQUENCE dmat_qa.users_user_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE dmat_qa.users_user_id_seq OWNER TO postgres;

ALTER TABLE ONLY dmat_qa.users ALTER COLUMN user_id SET DEFAULT nextval('dmat_qa.users_user_id_seq'::regclass);

ALTER TABLE dmat_qa.logs ADD COLUMN odlv_upload boolean NULL DEFAULT false;
ALTER TABLE dmat_qa.logs ADD COLUMN odlv_user_id integer NULL;
ALTER TABLE dmat_qa.logs ADD COLUMN odlv_onp_status character varying(50) NULL DEFAULT NULL::character varying;
ALTER TABLE dmat_qa.logs ADD COLUMN odlv_lv_status character varying(50) NULL DEFAULT NULL::character varying;
ALTER TABLE dmat_qa.logs ADD COLUMN zip_start_time timestamp without time zone NULL;
ALTER TABLE dmat_qa.logs ADD COLUMN zip_end_time timestamp without time zone NULL;
ALTER TABLE dmat_qa.logs ADD COLUMN cp_hdfs_start_time timestamp without time zone;
ALTER TABLE dmat_qa.logs ADD COLUMN cp_hdfs_end_time timestamp without time zone;
ALTER TABLE dmat_qa.logs ADD COLUMN is_deleted boolean NOT NULL DEFAULT false;
ALTER TABLE dmat_qa.logs ADD COLUMN device_os character varying(24) COLLATE pg_catalog."default" DEFAULT NULL::character varying;
ALTER TABLE dmat_qa.logs ADD COLUMN udid character varying(64) COLLATE pg_catalog."default" DEFAULT NULL::character varying;
ALTER TABLE dmat_qa.logs ADD COLUMN lv_file_proc_audit text COLLATE pg_catalog."default";
ALTER TABLE dmat_qa.logs ADD COLUMN nr_connectivity_mode character varying(50);
ALTER TABLE dmat_qa.logs ADD COLUMN vz_regions character varying;
ALTER TABLE dmat_qa.logs ADD COLUMN market_area character varying;

CREATE SEQUENCE dmat_qa.lv_filemetainfo_id_seq
    INCREMENT 1
    START 1
    MINVALUE 1
    MAXVALUE 9223372036854775807
    CACHE 1;

ALTER SEQUENCE dmat_qa.lv_filemetainfo_id_seq
    OWNER TO postgres;

CREATE TABLE dmat_qa.lv_filemetainfo
(
    file_name text COLLATE pg_catalog."default" NOT NULL,
    id bigint NOT NULL DEFAULT nextval('dmat_qa.lv_filemetainfo_id_seq'::regclass),
    logcodes character varying[] COLLATE pg_catalog."default",
    creation_time timestamp without time zone DEFAULT now(),
    trigger_count integer DEFAULT 0,
    CONSTRAINT lv_filemetainfo_pkey3 PRIMARY KEY (file_name)
);

ALTER TABLE dmat_qa.lv_filemetainfo
    OWNER to postgres;
