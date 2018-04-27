package web;



import java.util.ArrayList;
import java.util.List;


import com.google.inject.servlet.RequestParameters;

import models.Article;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;
import org.springframework.ui.Model;
import org.springframework.beans.factory.annotation.Autowired;
import query.SparkSearch;


@Controller
@RequestMapping("/")

public class SearchController {

	 @Autowired
	 SparkSearch service;
	
    @RequestMapping(method = RequestMethod.GET)
    public String index() {
        return "index";
    }

    @RequestMapping(value = "/search", method = RequestMethod.GET)
    public String Search(@RequestParam("term") String query, Model model) {
		List<Article> results = service.search(query);
        model.addAttribute("results", results);
        return "search";

    }


}