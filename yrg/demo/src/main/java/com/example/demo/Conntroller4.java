package com.example.demo;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
public class Conntroller4 {
    @CrossOrigin
    @RequestMapping("/login")
    public String index() {
        return "login";
    }
}