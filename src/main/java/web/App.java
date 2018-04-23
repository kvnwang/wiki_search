package web;

import static spark.Spark.*;

public class App {
    public static void main(String[] args) {
        get("/", (req, res) -> "Hello World");
    }
}