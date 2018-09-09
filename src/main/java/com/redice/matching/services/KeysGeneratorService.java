package com.redice.matching.services;

import com.redice.matching.entities.MatchRequest;
import com.redice.matching.entities.MatchRequestJoiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class KeysGeneratorService implements KeysGenerator {

    private Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public String composeComplexKey(MatchRequestJoiner matchRequestJoiner, MatchRequest matchRequest) {
        final String[] composedKey = {""};
        matchRequestJoiner.getKeysJoiner().forEach((k, v) -> {
            switch (k) {
                case "course":
                    if (!composedKey[0].isEmpty()) {
                        composedKey[0] = composedKey[0].concat(":".concat(matchRequest.getCourse()));
                    } else {
                        composedKey[0] = composedKey[0].concat(matchRequest.getCourse());
                    }
                    break;
                case "level":
                    int levelDivider = Integer.parseInt(v.getExtraData());
                    if (!composedKey[0].isEmpty()) {
                        composedKey[0] = composedKey[0].concat(":".concat(String.valueOf(Integer.parseInt(matchRequest.getLevel()) / levelDivider)));
                    } else {
                        composedKey[0] = composedKey[0].concat(String.valueOf(Integer.parseInt(matchRequest.getLevel()) / levelDivider));
                    }
                    break;
                default:
                    break;
            }
        });


        logger.info("generating key: " + composedKey[0]);
        return composedKey[0];
    }

}
