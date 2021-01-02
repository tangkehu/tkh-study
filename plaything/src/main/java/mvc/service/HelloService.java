package mvc.service;

import mvc.entity.Hello;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;


@RestController
@RequestMapping("/api")
public class HelloService {
    @Autowired
    Hello helloEntity;

    @GetMapping("/hello")
    public Hello hello() {
        return helloEntity;
    }
}
