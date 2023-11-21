package com.verizon.oneparser.datacopier.batch.datatype;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.util.CollectionUtils;

import javax.validation.constraints.NotNull;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class LogResponse {

    private List<LogItemReponse> resp;

    public int size() {
        return CollectionUtils.isEmpty(resp) ? 0 : resp.size();
    }

    public LogItemReponse getItem(int index) {
        return CollectionUtils.isEmpty(resp) ? null : resp.get(index);
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class LogItemReponse {
        @NotNull
        @JsonProperty("dm_user")
        private Integer dmUser;
        @NotNull
        @JsonProperty("file_name")
        private String fileName;
        @NotNull
        @JsonProperty("file_location")
        private String fileLocation;
    }
}
