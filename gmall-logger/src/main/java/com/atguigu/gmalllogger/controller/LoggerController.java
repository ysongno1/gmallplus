package com.atguigu.gmalllogger.controller;

import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Slf4j
public class LoggerController {

    @Autowired
    private KafkaTemplate<String,String> kafkaTemplate;

//    @RequestMapping("test1")
//    //    @ResponseBody
//    public String test1() {
//        System.out.println("11111111");
//        return "success";
//    }


    @RequestMapping("applog")
    public String getLogger(@RequestParam("param") String jsonStr){


//        System.out.println(jsonStr);

        //操作取决于logback.xml文件
        // 落盘第一种方法获取日志信息
//        Logger logger = LoggerFactory.getLogger(LoggerController.class);
//        logger.info(jsonStr);

        //落盘第二种
        log.info(jsonStr);

        //发送到kafka上
        kafkaTemplate.send("ods_base_log",jsonStr);

        return "success";
    }
}
