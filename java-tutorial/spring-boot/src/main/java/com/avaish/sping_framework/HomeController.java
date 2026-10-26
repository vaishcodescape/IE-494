//Home page controller in Java

package com.avaish.spring_framework;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController //Rest Controller annotation in Java

public class HomeController {

    @GetMapping("/")
    public String index() {
        return "Hello from Spring Boot!"; // sent to the browser as-is
    }
}
