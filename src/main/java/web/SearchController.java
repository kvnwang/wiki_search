package web;



import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import query.Query;


@Controller
public class SearchController {
	
    @GetMapping("/")
    public String home(@RequestParam(name="name", required=false, defaultValue="World") String name, Model model) {
		model.addAttribute("name", name);
        return "index";
    }

    @RequestMapping(value = "/search")
    public String Search(@RequestParam("term") String term, Model model) {
    	Query query=new Query();
        model.addAttribute("results", term);
        List list=new ArrayList<String>();
        System.out.println(term);
        if(term != null){
            String result=query.search(term);
            model.addAttribute("term", term);  
            model.addAttribute("results", result);
        }
        System.out.println();
        System.out.println();
        System.out.println("a");
        System.out.println();
        System.out.println();

        return "search";
    }    
    
    
    

}