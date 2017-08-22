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

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

@Service
public class KitAwareClassLoaderDelegator {

  private ClassLoader urlClassLoader = Thread.currentThread().getContextClassLoader();

  @Autowired
  private Settings settings;

  @PostConstruct
  public void init() {
    String kitPath = settings.getKitPath();
    if (kitPath != null) {
      try {
        URL[] urls = discoverKitClientJars(kitPath);
        urlClassLoader = URLClassLoader.newInstance(urls, Thread.currentThread().getContextClassLoader());
      } catch (Exception e) {
        settings.setKitPath(null); // clear the kit path so that at next startup it is not called with an invalid one
      }
    }
  }

  public ClassLoader getUrlClassLoader() {
    return urlClassLoader;
  }

  private static URL[] discoverKitClientJars(String kitPath) throws Exception {
    Stream<Path> clientEhcacheJarStream = null;
    try {
      clientEhcacheJarStream = Files.walk(Paths.get(kitPath, "/client/ehcache"), 4, FileVisitOption.FOLLOW_LINKS)
          .filter(path -> path.toFile().getAbsolutePath().endsWith("jar"));
    } catch (IOException e) {
      // that's fine, it's probably just a kit without ehcache
    }
    Stream<Path> clientStoreJarStream = null;
    try {
      clientStoreJarStream = Files.walk(Paths.get(kitPath, "/client/store"), 4, FileVisitOption.FOLLOW_LINKS)
          .filter(path -> path.toFile().getAbsolutePath().endsWith("jar"));
    } catch (IOException e) {
      // that's fine, it's probably just a kit without tc store
    }
    Stream<Path> clientLibJarStream = Stream.empty();
    try {
      clientLibJarStream = Files.walk(Paths.get(kitPath, "/client/lib"), 4, FileVisitOption.FOLLOW_LINKS)
          .filter(path -> path.toFile().getAbsolutePath().endsWith("jar"));
    } catch (IOException e) {
      // it's possible that the client libs are elsewhere...
      clientLibJarStream = Files.walk(Paths.get(kitPath, "../common/lib"), 4, FileVisitOption.FOLLOW_LINKS)
          .filter(path -> path.toFile().getAbsolutePath().endsWith("-client.jar"));
    }
    Stream<Path> concat = clientEhcacheJarStream != null ? Stream.concat(clientEhcacheJarStream, clientLibJarStream) : clientLibJarStream;
    concat = clientStoreJarStream != null ? Stream.concat(concat, clientStoreJarStream) : concat;
    return concat.map(path -> {
      try {
        return new URL("file:" + path.toFile().getAbsolutePath());
      } catch (MalformedURLException e) {
        e.printStackTrace();
      }
      return null;
    }).toArray(URL[]::new);
  }

  public boolean containsTerracottaStore() {
    try {
      urlClassLoader.loadClass("com.terracottatech.store.manager.DatasetManager");
      return true;
    } catch (ClassNotFoundException e) {
      return false;
    }
  }

  public boolean containsEhcache() {
    try {
      urlClassLoader.loadClass("org.ehcache.CacheManager");
      return true;
    } catch (ClassNotFoundException e) {
      return false;
    }
  }

  public boolean isEEKit() {
    try {
      urlClassLoader.loadClass("com.terracottatech.ehcache.clustered.client.internal.config.EnterpriseClusteredDedicatedResourcePoolImpl");
      return true;
    } catch (ClassNotFoundException e) {
      return false;
    }
  }

  public void setKitPath(String kitPath) {
    settings.setKitPath(kitPath);
    if (kitPath != null && !kitPath.isEmpty()) {
      try {
        URL[] urls = discoverKitClientJars(kitPath);
        urlClassLoader = URLClassLoader.newInstance(urls, Thread.currentThread().getContextClassLoader());
      } catch (Exception e) {
        throw new RuntimeException("Please make sure the kitPath is properly set, current value is : " + kitPath, e);
      }
    }
  }
}
