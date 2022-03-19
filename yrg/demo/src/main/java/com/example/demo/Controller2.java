package com.example.demo;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
public class Controller2 {
    @CrossOrigin
    @RequestMapping("/group")
    public String index() {
        return "admin-user";
    }
}