package com.example.demo;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;
import java.io.IOException;
import java.util.HashMap;

import static sparks.GetMsguser.getMsguser;
import static sparks.Getagebr.getagebr;
import static sparks.Getconrf.getconrf;
import static sparks.Getcycle.getcycle;
import static sparks.Getgdage.getgdage;
import static sparks.Getjobrf.getjobrf;
import static sparks.Getlogjob.getlogjob;
import static sparks.Getmuser.getmuser;
import static sparks.Getperpro.getperpro;
import static sparks.Getprovrf.getprovrf;
import static sparks.Getscantime.getusct;
import static sparks.Getucon.getucon;
import static sparks.Getutitle.getutitle;

@Controller
public class Controller1 {
    @CrossOrigin
    @RequestMapping("/single")
    public String index() {
        return "index";
    }
}

@RestController
class user {
    @CrossOrigin
    @RequestMapping(value = "/userprofile", method = RequestMethod.GET)
    public HashMap<String, String> indexum(@RequestParam("msg") String msg) throws IOException {
        String data = getMsguser(msg);
        System.out.println(data);
        HashMap<String, String> hashmap = new HashMap<String, String>();
        hashmap.put("msg", data);
        return hashmap;
    }

    @CrossOrigin
    @RequestMapping(value = "/usercycle", method = RequestMethod.GET)
    public HashMap<String, String> indexucy(@RequestParam("msg") String msg) throws IOException {
        String data = getcycle(msg);
        HashMap<String, String> hashmap = new HashMap<String, String>();
        hashmap.put("msg", data);
        return hashmap;
    }

    @CrossOrigin
    @RequestMapping(value = "/userconsum", method = RequestMethod.GET)
    public HashMap<String, String> indexucm(@RequestParam("msg") String msg) throws IOException {
        String data = getucon(msg);
        HashMap<String, String> hashmap = new HashMap<String, String>();
        hashmap.put("msg", data);
        return hashmap;
    }

    @CrossOrigin
    @RequestMapping(value = "/usersctime", method = RequestMethod.GET)
    public HashMap<String, String> indexusct(@RequestParam("msg") String msg) throws IOException {
        String data = getusct(msg);
        HashMap<String, String> hashmap = new HashMap<String, String>();
        hashmap.put("msg", data);
        return hashmap;
    }

    @CrossOrigin
    @RequestMapping(value = "/userperpro", method = RequestMethod.GET)
    public HashMap<String, String> indexupp(@RequestParam("msg") String msg) throws IOException {
        String data = getperpro(msg);
        HashMap<String, String> hashmap = new HashMap<String, String>();
        hashmap.put("msg", data);
        return hashmap;
    }

    @CrossOrigin
    @RequestMapping(value = "/usertitle", method = RequestMethod.GET)
    public HashMap<String, String> indexutit(@RequestParam("msg") String msg) throws IOException {
        String data = getutitle(msg);
        HashMap<String, String> hashmap = new HashMap<String, String>();
        hashmap.put("msg", data);
        return hashmap;
    }
}


@RestController
class people{
    @CrossOrigin
    @RequestMapping(value = "/peoplega", method = RequestMethod.GET)
    public HashMap<String, String> indexpga( ) throws IOException {
        String data = getgdage();
        HashMap<String, String> hashmap = new HashMap<String, String>();
        hashmap.put("msg", data);
        return hashmap;
    }

    @CrossOrigin
    @RequestMapping(value = "/peprovrf", method = RequestMethod.GET)
    public HashMap<String, String> indexpprf() throws IOException {
        String data = getprovrf();
        HashMap<String, String> hashmap = new HashMap<String, String>();
        hashmap.put("msg", data);
        return hashmap;
    }

    @CrossOrigin
    @RequestMapping(value = "/peoplecrf", method = RequestMethod.GET)
    public HashMap<String, String> indexcrf() throws IOException {
        String data = getconrf();
        HashMap<String, String> hashmap = new HashMap<String, String>();
        hashmap.put("msg", data);
        return hashmap;
    }

    @CrossOrigin
    @RequestMapping(value = "/peoplejrf", method = RequestMethod.GET)
    public HashMap<String, String> indexjrf() throws IOException {
        String data = getjobrf();
        HashMap<String, String> hashmap = new HashMap<String, String>();
        hashmap.put("msg", data);
        return hashmap;
    }

    @CrossOrigin
    @RequestMapping(value = "/peopleagebr", method = RequestMethod.GET)
    public HashMap<String, String> indexagebe() throws IOException {
        String data = getagebr();
        HashMap<String, String> hashmap = new HashMap<String, String>();
        hashmap.put("msg", data);
        return hashmap;
    }

    @CrossOrigin
    @RequestMapping(value = "/peoplogjob", method = RequestMethod.GET)
    public HashMap<String, String> indexplj() throws IOException {
        String data = getlogjob();
        HashMap<String, String> hashmap = new HashMap<String, String>();
        hashmap.put("msg", data);
        return hashmap;
    }
}




@RestController
class muser {
    @CrossOrigin
    @RequestMapping(value = "/usermore", method = RequestMethod.POST)
    public HashMap<String, String> indexmuser(@RequestBody String msg) throws IOException {
        String data = getmuser(msg);
        HashMap<String, String> hashmap = new HashMap<String, String>();
        hashmap.put("msg", data);
        return hashmap;
    }
}
