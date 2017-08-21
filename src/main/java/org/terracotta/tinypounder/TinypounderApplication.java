/*
 * Copyright Terracotta, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.terracotta.tinypounder;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

@SpringBootApplication
@Configuration
public class TinypounderApplication {

  @Value("${kitPath}")
  private String kitPath;

  public static void main(String[] args) throws Exception {
    SpringApplication.run(TinypounderApplication.class, args);
  }

  @Bean
  public KitAwareClassLoaderDelegator initializeKitAwareClassLoader() {
    KitAwareClassLoaderDelegator kitAwareClassLoaderDelegator = new KitAwareClassLoaderDelegator();
    if (kitPath != null && kitPath.length() > 0) {
      try {
        kitAwareClassLoaderDelegator.setKitPathAndUpdate(kitPath);
      } catch (Exception e) {
        new RuntimeException("Please make sure the kitPath is properly set, current value is : " + kitPath, e);
      }
    }
    return kitAwareClassLoaderDelegator;
  }

  @Bean(destroyMethod = "shutdownNow")
  public ScheduledExecutorService scheduledExecutorService() {
    return Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors(), r -> {
      Thread t = new Thread(r);
      t.setDaemon(true);
      return t;
    });
  }

}
