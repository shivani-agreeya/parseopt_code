package com.verizon.oneparser.hdfscopier.web;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.util.CollectionUtils;

import javax.validation.constraints.NotNull;
import java.sql.Timestamp;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class DbGwResponse {
    private List<DbGwItemReponse> resp;

    public int size() {
        return CollectionUtils.isEmpty(resp) ? 0 : resp.size();
    }

    public DbGwItemReponse getItem(int index) {
        return CollectionUtils.isEmpty(resp) ? null : resp.get(index);
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class DbGwItemReponse {
        @NotNull
        private Long id;
        @NotNull
        @JsonProperty("dm_user")
        private Integer dmUser;
        @NotNull
        @JsonProperty("file_name")
        private String fileName;
        @NotNull
        @JsonProperty("file_location")
        private String fileLocation;

        private String filetype;
        private Double size;
        private Timestamp creationTime;
        private Timestamp updatedTime;
        private Timestamp cpHdfsTime;
        private Boolean compressionstatus;
    }
}
