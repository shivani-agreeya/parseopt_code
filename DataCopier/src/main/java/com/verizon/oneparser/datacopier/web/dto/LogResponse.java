package com.verizon.oneparser.datacopier.web.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class LogResponse {

    private List<LogItemReponse> resp;

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class LogItemReponse {
        @JsonProperty("dm_user")
        private Integer dmUser;
        @JsonProperty("file_name")
        private String fileName;
        @JsonProperty("file_location")
        private String fileLocation;
        @JsonProperty("creation_time")
        private String creationTime;
    }
}
