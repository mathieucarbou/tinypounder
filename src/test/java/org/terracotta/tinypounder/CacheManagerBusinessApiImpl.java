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

import org.ehcache.Cache;
import org.ehcache.CachePersistenceException;
import org.ehcache.PersistentCacheManager;
import org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder;
import org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.core.EhcacheManager;
import org.ehcache.core.HumanReadable;
import org.ehcache.impl.config.persistence.CacheManagerPersistenceConfiguration;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Comparator;

import static org.ehcache.config.builders.ResourcePoolsBuilder.heap;
import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;


/**
 * This class is only here as an example of what would an API impl look like
 */

public class CacheManagerBusinessApiImpl implements CacheManagerBusiness {

  private EhcacheManager cacheManager;


  @Override
  public Collection<String> retrieveCacheNames() {
    return cacheManager.getRuntimeConfiguration().getCacheConfigurations().keySet();
  }


  private CacheConfiguration defaultCacheConfigurationHeapOffHeapDedicatedClustered() {
    return CacheConfigurationBuilder.newCacheConfigurationBuilder(
        String.class, String.class,
        newResourcePoolsBuilder().
            heap(1000L, MemoryUnit.KB)
            .offheap(10, MemoryUnit.MB)
            .with(ClusteredResourcePoolBuilder.clusteredDedicated("primary-server-resource", 20, MemoryUnit.MB)))
        .build();
  }

  private void deleteFolder(String tinyPounderDiskPersistenceLocation) throws IOException {
    Path rootPath = Paths.get(tinyPounderDiskPersistenceLocation);
    Files.walk(rootPath, FileVisitOption.FOLLOW_LINKS)
        .sorted(Comparator.reverseOrder())
        .map(Path::toFile)
        .peek(System.out::println)
        .forEach(File::delete);
  }

  @Override
  public void createCache(String alias, org.terracotta.tinypounder.CacheConfiguration cacheConfiguration) {
    cacheManager.createCache(alias, defaultCacheConfigurationHeapOffHeapDedicatedClustered());
  }

  @Override
  public void close() {
    cacheManager.close();
  }

  @Override
  public void destroy() {
    try {
      cacheManager.destroy();
    } catch (CachePersistenceException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void destroyCache(String alias) {
    try {
      cacheManager.destroyCache(alias);
    } catch (CachePersistenceException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void removeCache(String alias) {
    cacheManager.removeCache(alias);
  }

  @Override
  public String retrieveHumanReadableConfiguration() {
    return ((HumanReadable) cacheManager.getRuntimeConfiguration()).readableString();
  }

  @Override
  public void initializeCacheManager(String terracottaServerUrl, String cmName, String tinyPounderDiskPersistenceLocation) {
    URI clusterUri = URI.create("terracotta://" + terracottaServerUrl + "/" + cmName);


    File tinyPounderDiskPersistenceLocationFolder = new File(tinyPounderDiskPersistenceLocation);
    if (tinyPounderDiskPersistenceLocationFolder.exists()) {
      try {
        deleteFolder(tinyPounderDiskPersistenceLocation);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    CacheManagerBuilder<PersistentCacheManager> cacheManagerBuilder =
        CacheManagerBuilder.newCacheManagerBuilder()
            .with(ClusteringServiceConfigurationBuilder.cluster(clusterUri)
                .autoCreate().defaultServerResource("primary-server-resource")
                .resourcePool("resource-pool-a", 128, MemoryUnit.MB, "secondary-server-resource")
                .resourcePool("resource-pool-b", 64, MemoryUnit.MB)
            ).with(new CacheManagerPersistenceConfiguration(tinyPounderDiskPersistenceLocationFolder))
//            .using(new DefaultManagementRegistryConfiguration()
//                .addTags("tiny", "pounder", "client")
//                .setCacheManagerAlias(cmName)
//            )
        ;

    Cache<Integer, String> cache = cacheManager.getCache(cmName, Integer.class, String.class);
//cache.put();
    EhcacheManager cacheManager = (EhcacheManager) cacheManagerBuilder.build();
    cacheManager.init();
    this.cacheManager = cacheManager;
  }

  @Override
  public boolean isCacheManagerAlive() {
    return false;
  }

  @Override
  public String getStatus() {
    return null;
  }

  @Override
  public void updatePoundingIntensity(String cacheAlias, int poundingIntensity) {

  }

  @Override
  public int retrievePoundingIntensity(String cacheAlias) {
    return 0;
  }
}
