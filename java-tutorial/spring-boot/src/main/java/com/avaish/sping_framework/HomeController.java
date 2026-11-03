//Home page controller in Java

package com.avaish.spring_framework;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController //Rest Controller annotation in Java to create a backend service

public class HomeController {

    @GetMapping("/")
    public String index() {
        return "Hello from Spring Boot!";        
        String viewname = "index"; // view name to be returned
        

    @PostRequestMapping("/submit")
    public String submitForm(@RequestParam("name") String name, @RequestParam("email") String email) {
        // Process the form data (e.g., save to database, send email, etc.)
        // For demonstration purposes, we'll just return a success message
        return "Form submitted successfully! Name: " + name + ", Email: " + email
    }
}       
}