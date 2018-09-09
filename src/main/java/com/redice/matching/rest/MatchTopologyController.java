package com.redice.matching.rest;

import com.redice.matching.entities.MatchRequestJoiner;
import com.redice.matching.services.KeysGenerator;
import com.redice.matching.topology.MatchTopologyExecutor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/matching/api")
public class MatchTopologyController {

    @Autowired
    private MatchTopologyExecutor matchTopologyExecutor;

    @Autowired
    private KeysGenerator keysGenerator;

    @RequestMapping(value = "/deploy", method = RequestMethod.POST)
    public void deployMatchingTopology(@RequestParam(value = "pipe_name") final String pipeName,
                                       @RequestBody MatchRequestJoiner matchRequestJoiner) {
        matchTopologyExecutor.deployMatchTopology(pipeName, matchRequestJoiner);
    }

}
