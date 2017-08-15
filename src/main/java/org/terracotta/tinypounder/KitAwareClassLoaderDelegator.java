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

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

public class KitAwareClassLoaderDelegator {

  public ClassLoader getUrlClassLoader() {
    return urlClassLoader;
  }

  private ClassLoader urlClassLoader;

  public String getKitPath() {
    return kitPath;
  }

  private String kitPath;

  public KitAwareClassLoaderDelegator() {
    urlClassLoader = Thread.currentThread().getContextClassLoader();
  }

  private static URL[] discoverKitClientJars(String kitPath) throws Exception {
    Stream<Path> clientEhcacheJarStream = Files.walk(Paths.get(kitPath, "/client/ehcache"), 4, FileVisitOption.FOLLOW_LINKS)
        .filter(path -> path.toFile().getAbsolutePath().endsWith("jar"));
    Stream<Path> clientStoreJarStream = Files.walk(Paths.get(kitPath, "/client/store"), 4, FileVisitOption.FOLLOW_LINKS)
        .filter(path -> path.toFile().getAbsolutePath().endsWith("jar"));
    Stream<Path> clientLibJarStream = Files.walk(Paths.get(kitPath, "/client/lib"), 4, FileVisitOption.FOLLOW_LINKS)
        .filter(path -> path.toFile().getAbsolutePath().endsWith("jar"));
    return Stream.concat(Stream.concat(clientEhcacheJarStream, clientLibJarStream), clientStoreJarStream).map(path -> {
      try {
        return new URL("file:" + path.toFile().getAbsolutePath());
      } catch (MalformedURLException e) {
        e.printStackTrace();
      }
      return null;
    }).toArray(URL[]::new);
  }

  public void setKitPathAndUpdate(String kitPath) throws Exception {
    this.kitPath = kitPath;
    URL[] urls = discoverKitClientJars(kitPath);
    urlClassLoader = URLClassLoader.newInstance(urls, Thread.currentThread().getContextClassLoader());
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

}
