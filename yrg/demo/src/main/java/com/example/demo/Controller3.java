package com.example.demo;

import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.servlet.ModelAndView;

import java.io.IOException;
import java.util.HashMap;

import static sparks.Getscantime.getusct;

@Controller
public class Controller3 {
    @CrossOrigin
    @RequestMapping("/search")
    public String index() {
        return "admin-table";
    }
}

@Controller
class Controlleruserinfo {
    @CrossOrigin
    @RequestMapping(value = "/userinfomation", method = RequestMethod.GET)
    public String index(@RequestParam("msg") String msg, ModelMap model) {
        model.addAttribute("msg",msg);
        return "userinfo";
    }
}
