package query;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Stack;

import org.tartarus.snowball.ext.PorterStemmer;

public class QueryParser {
    public static HashSet<String> operatorTerms = new HashSet<String>();
	public PorterStemmer stemmer = new PorterStemmer();

    public QueryParser () {
        initHash();
    }
    private static void initHash() {
        operatorTerms.add("NOT");
        operatorTerms.add("AND");
        operatorTerms.add("OR");
    }

    // =========================================================================================
    // for a given query, returns a stack containing the query broken up with each
    // entry either
    // a parenthesis or a word
    // **THE WAY I HAVE IT SET, YOU CANNOT INPUT WORDS WITH SPACES (eg. Stephen
    // Curry)
    // =========================================================================================

    public ArrayList<String> convert (String query) {
        String[] tempArray = query.split(" ");
        Stack<String> workingStack = new Stack<String>();
        Stack<String> operators = new Stack<String>();
        ArrayList<String> output = new ArrayList<String>();
        int i = 0;
        for (String str : tempArray) {
			stemmer.setCurrent(str);
		    stemmer.stem();
		    String s=stemmer.getCurrent().trim();
		    
            if (s.equals("(")) {
                workingStack.push(s);
            } else if (s.equals(")")) {
                if (operatorTerms.contains(workingStack.peek())) {
                    if (!operators.isEmpty()) {
                        output.add(operators.pop());
                        if (!workingStack.isEmpty()) operators.push(workingStack.pop());
                    } else {
                        if (!workingStack.isEmpty()) operators.push(workingStack.pop());
                        while (!operatorTerms.contains(workingStack.peek()) && !workingStack.peek().equals("(")) {
                            if (!workingStack.isEmpty()) output.add(workingStack.pop());
                        }
                    }
                    if (!workingStack.isEmpty()) workingStack.pop();
                }else {
                    if (!workingStack.isEmpty()) output.add(workingStack.pop());
                    String operation = workingStack.pop();
                    if (!workingStack.isEmpty()) output.add(workingStack.pop());
                    output.add(operation);
                    if (!workingStack.isEmpty()) workingStack.pop();
                }
            } else {
                workingStack.push(s);
            }
            i++;
        }
        while (!workingStack.isEmpty()) {
            if (!workingStack.peek().equals("C")) output.add(workingStack.pop());
        }
        if(!operators.isEmpty()) output.add(operators.pop());
        for (String S : output) {
            System.out.println(S);
        }
        return output;
    }

}
