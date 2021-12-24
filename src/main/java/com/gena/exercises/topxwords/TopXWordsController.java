package com.gena.exercises.topxwords;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;


@RestController
public class TopXWordsController {

    @Autowired
    private SparkService sparkService;

    @GetMapping("/topX")
    public ResponseEntity<Map<String, Integer>> topX(
            @RequestParam("fileName") String fileName,
            @RequestParam(name = "top", required = false, defaultValue = "5") int top) {
        scala.collection.immutable.Map<String, Object> scalaResult = sparkService.countTopX(fileName, top);
        return new ResponseEntity<>(Converter.convert(scalaResult), HttpStatus.OK);
    }

}
