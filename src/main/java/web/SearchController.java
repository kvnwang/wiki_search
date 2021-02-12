package web;



import java.util.List;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;


@Controller
@RequestMapping("/")

public class SearchController {
	
	
    @RequestMapping(method = RequestMethod.GET)
    public String index() {
        return "index";
    }

    @RequestMapping(value = "/search", method = RequestMethod.GET)
    public String Search(@RequestParam("term") String query, Model model) {
		SparkSearch service=new SparkSearch();
		List<Article> results = service.search(query);
        model.addAttribute("results", results);
        return "search";

    }


}