package com.proj.dataflow.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import com.proj.dataflow.service.DataFlowService;

@RestController
public class DataflowController {

	@Autowired
	private DataFlowService dataflowService;


    @GetMapping("/runpipeline")
    public String triggerDataFlow() {
    	dataflowService.triggerDataflowLocal();
        return "Beam pipeline executed!";
    }
}
