package com.verizon.oneparser.hdfscopier.web;

import com.verizon.oneparser.hdfscopier.config.JobProperties;
import com.verizon.oneparser.hdfscopier.persistence.entity.Log;
import com.verizon.oneparser.hdfscopier.persistence.repository.LogRepository;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Profile;
import org.springframework.data.domain.PageRequest;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Profile("dbgw-stub")
@RestController
@RequestMapping("dbgw/api/v1/logs")
@AllArgsConstructor
public class DbGwController {

    private final JobProperties jobProperties;
    private final LogRepository repository;

    @GetMapping(value="/{filetype}/{limit}/{env}", produces = "application/json")     //dbgw/api/v1/logs/dlf/5/DEV
    public DbGwResponse listUsersInvoices(
            @PathVariable("filetype") String filetype,
            @PathVariable("limit") String limit,
            @PathVariable("env") String env) {
        int querySize = jobProperties.getDbgwLimit();
        try {
            querySize = Integer.parseInt(limit);
        } catch (NumberFormatException e) {
            log.error("Failed to parse DB Gateway query limit,  query limit is set to default " + querySize);
        }
        List<Log> logs = repository.findByStatusAndFiletypeAndProcessingServerOrderByIdAsc("Available", filetype, env, PageRequest.of(0, querySize));
        DbGwResponse dbGwResponse = new DbGwResponse(logs.stream().map(log -> new DbGwResponse.DbGwItemReponse(log.getId(), log.getDmUser(), log.getFileName(), log.getFileLocation(), log.getFiletype(), log.getSize(), log.getCreationTime(), log.getUpdatedTime(), log.getCpHdfsTime(), log.getCompressionstatus())).collect(Collectors.toList()));
        log.debug("dbGwResponse: " + dbGwResponse);
        return dbGwResponse;
    }
}
