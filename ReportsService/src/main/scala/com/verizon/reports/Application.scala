package com.verizon.reports

import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.scheduling.annotation.EnableAsync

@SpringBootApplication(scanBasePackages = Array("com.verizon"))
@EnableAsync(proxyTargetClass=true )
class Application

object Application extends App {
  SpringApplication.run(classOf[Application])
}
