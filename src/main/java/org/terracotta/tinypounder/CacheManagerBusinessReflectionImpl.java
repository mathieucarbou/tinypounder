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

import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Comparator;
import java.util.Map;

@Service
public class CacheManagerBusinessReflectionImpl implements CacheManagerBusiness {


  public static final String NO_CACHE_MANAGER = "NO CACHE MANAGER";
  private final KitAwareClassLoaderDelegator kitAwareClassLoaderDelegator;
  private Object cacheManager;
  private Class<?> ehCacheManagerClass;

  public CacheManagerBusinessReflectionImpl(KitAwareClassLoaderDelegator kitAwareClassLoaderDelegator) throws Exception {
    this.kitAwareClassLoaderDelegator = kitAwareClassLoaderDelegator;
  }

  private Class loadClass(String className) throws ClassNotFoundException {
    return kitAwareClassLoaderDelegator.getUrlClassLoader().loadClass(className);
  }

  @Override
  public Collection<String> retrieveCacheNames() {

    try {
      Thread.currentThread().setContextClassLoader(kitAwareClassLoaderDelegator.getUrlClassLoader());

      Class<?> configurationClass = loadClass("org.ehcache.config.Configuration");

      Method getRuntimeConfigurationMethod = ehCacheManagerClass.getMethod("getRuntimeConfiguration");
      Object runtimeConfiguration = getRuntimeConfigurationMethod.invoke(cacheManager);
      Method readableStringMethod = configurationClass.getMethod("getCacheConfigurations");
      Map cacheConfigurations = (Map) readableStringMethod.invoke(runtimeConfiguration);
      return cacheConfigurations.keySet();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void createCache(String alias) {
    try {
      Thread.currentThread().setContextClassLoader(kitAwareClassLoaderDelegator.getUrlClassLoader());
      Class<?> cacheConfigurationClass = loadClass("org.ehcache.config.CacheConfiguration");
      Method createCacheMethod = ehCacheManagerClass.getMethod("createCache", String.class, cacheConfigurationClass);
      createCacheMethod.invoke(cacheManager, alias, defaultCacheConfigurationHeapOffHeapDedicatedClustered());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {
    try {
      Thread.currentThread().setContextClassLoader(kitAwareClassLoaderDelegator.getUrlClassLoader());
      Method closeMethod = ehCacheManagerClass.getMethod("close");
      closeMethod.invoke(cacheManager);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void destroy() {
    try {
      Thread.currentThread().setContextClassLoader(kitAwareClassLoaderDelegator.getUrlClassLoader());
      Method destroyMethod = ehCacheManagerClass.getMethod("destroy");
      destroyMethod.invoke(cacheManager);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void destroyCache(String alias) {
    try {
      Thread.currentThread().setContextClassLoader(kitAwareClassLoaderDelegator.getUrlClassLoader());
      Method destroyCacheMethod = ehCacheManagerClass.getMethod("destroyCache", String.class);
      destroyCacheMethod.invoke(cacheManager, alias);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void removeCache(String alias) {
    try {
      Thread.currentThread().setContextClassLoader(kitAwareClassLoaderDelegator.getUrlClassLoader());
      Method removeCacheMethod = ehCacheManagerClass.getMethod("removeCache", String.class);
      removeCacheMethod.invoke(cacheManager, alias);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String retrieveHumanReadableConfiguration() {
    try {
      Thread.currentThread().setContextClassLoader(kitAwareClassLoaderDelegator.getUrlClassLoader());

      Class<?> humanReadableClass = loadClass("org.ehcache.core.HumanReadable");

      Method getRuntimeConfigurationMethod = ehCacheManagerClass.getMethod("getRuntimeConfiguration");
      Object runtimeConfiguration = getRuntimeConfigurationMethod.invoke(cacheManager);
      Method readableStringMethod = humanReadableClass.getMethod("readableString");
      return (String) readableStringMethod.invoke(runtimeConfiguration);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

  }

  @Override
  public void initializeCacheManager(String terracottaServerUrl, String cmName, String tinyPounderDiskPersistenceLocation) {

    URI clusterUri = URI.create("terracotta://" + terracottaServerUrl + "/" + cmName);


    File tinyPounderDiskPersistenceLocationFolder = new File(tinyPounderDiskPersistenceLocation);
    if (tinyPounderDiskPersistenceLocationFolder.exists()) {
      try {
        deleteFolder(tinyPounderDiskPersistenceLocation);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    try {
      Thread.currentThread().setContextClassLoader(kitAwareClassLoaderDelegator.getUrlClassLoader());
      Object clusteringServiceConfigurationBuilder = constructEnterpriseClusteringServiceConfigurationBuilder(cmName, clusterUri, kitAwareClassLoaderDelegator.isEEKit());
      Object cacheManagerPersistenceConfiguration = constructCacheManagerPersistenceConfiguration(tinyPounderDiskPersistenceLocationFolder);
      Object cacheManager;
      if (kitAwareClassLoaderDelegator.isEEKit()) {
        Object defaultManagementRegistryConfiguration = constructDefaultManagementRegistryConfiguration(cmName);
        cacheManager = constructCacheManagerBuilder(clusteringServiceConfigurationBuilder, cacheManagerPersistenceConfiguration, defaultManagementRegistryConfiguration);
      } else {
        cacheManager = constructCacheManagerBuilder(clusteringServiceConfigurationBuilder, cacheManagerPersistenceConfiguration, null);
      }
      ehCacheManagerClass = loadClass("org.ehcache.core.EhcacheManager");
      Method initMethod = ehCacheManagerClass.getMethod("init");
      initMethod.invoke(cacheManager);
      this.cacheManager = cacheManager;

    } catch (Exception e) {
      throw new RuntimeException(e);
    }

  }

  private Object constructDefaultManagementRegistryConfiguration(String cmName) throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
    Class<?> defaultManagementRegistryConfigurationClass = loadClass("org.ehcache.management.registry.DefaultManagementRegistryConfiguration");
    Constructor defaultManagementRegistryConfigurationConstructor = defaultManagementRegistryConfigurationClass.getConstructor();
    Object defaultManagementRegistryConfiguration = defaultManagementRegistryConfigurationConstructor.newInstance();
    Method addTagMethod = defaultManagementRegistryConfigurationClass.getMethod("addTag", String.class);
    defaultManagementRegistryConfiguration = addTagMethod.invoke(defaultManagementRegistryConfiguration, "tiny");
    defaultManagementRegistryConfiguration = addTagMethod.invoke(defaultManagementRegistryConfiguration, "pounder");
    defaultManagementRegistryConfiguration = addTagMethod.invoke(defaultManagementRegistryConfiguration, "client");

    Method setCacheManagerAliasMethod = defaultManagementRegistryConfigurationClass.getMethod("setCacheManagerAlias", String.class);
    defaultManagementRegistryConfiguration = setCacheManagerAliasMethod.invoke(defaultManagementRegistryConfiguration, cmName);
    return defaultManagementRegistryConfiguration;
  }

  private Object constructCacheManagerPersistenceConfiguration(File tinyPounderDiskPersistenceLocationFolder) throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
    Class<?> cacheManagerPersistenceConfigurationClass = loadClass("org.ehcache.impl.config.persistence.CacheManagerPersistenceConfiguration");
    Constructor cacheManagerPersistenceConfigurationConstructor = cacheManagerPersistenceConfigurationClass.getConstructor(File.class);
    return cacheManagerPersistenceConfigurationConstructor.newInstance(tinyPounderDiskPersistenceLocationFolder);
  }

  private Object constructCacheManagerBuilder(Object enterpriseClusteringServiceConfigurationBuilder, Object cacheManagerPersistenceConfiguration, Object defaultManagementRegistryConfiguration) throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
    Class<?> cacheManagerBuilderClass = loadClass("org.ehcache.config.builders.CacheManagerBuilder");
    Method newCacheManagerBuilderMethod = cacheManagerBuilderClass.getMethod("newCacheManagerBuilder");
    Class<?> builderClass = loadClass("org.ehcache.config.Builder");
    Method withBuilderMethod = cacheManagerBuilderClass.getMethod("with", builderClass);
    Class<?> cacheManagerConfigurationClass = loadClass("org.ehcache.config.builders.CacheManagerConfiguration");
    Method withConfigurationMethod = cacheManagerBuilderClass.getMethod("with", cacheManagerConfigurationClass);
    Class<?> serviceCreationConfigurationClass = loadClass("org.ehcache.spi.service.ServiceCreationConfiguration");


    Object cacheManagerBuilder = newCacheManagerBuilderMethod.invoke(null);
    cacheManagerBuilder = withBuilderMethod.invoke(cacheManagerBuilder, enterpriseClusteringServiceConfigurationBuilder);
    cacheManagerBuilder = withConfigurationMethod.invoke(cacheManagerBuilder, cacheManagerPersistenceConfiguration);
    if (defaultManagementRegistryConfiguration != null) {
      Method usingMethod = cacheManagerBuilderClass.getMethod("using", serviceCreationConfigurationClass);
      cacheManagerBuilder = usingMethod.invoke(cacheManagerBuilder, defaultManagementRegistryConfiguration);
    }
    Method buildMethod = cacheManagerBuilderClass.getMethod("build");
    return buildMethod.invoke(cacheManagerBuilder);
  }

  private Object constructEnterpriseClusteringServiceConfigurationBuilder(String cmName, URI clusterUri, boolean eeKit) throws IllegalAccessException, InvocationTargetException, ClassNotFoundException, NoSuchMethodException {

    Class<?> memoryUnitClass = loadClass("org.ehcache.config.units.MemoryUnit");
    Method valueOfMethod = memoryUnitClass.getMethod("valueOf", String.class);
    Object mB = valueOfMethod.invoke(null, "MB");

    if (eeKit) {

      Class<?> enterpriseServerSideConfigurationBuilderClass = loadClass("com.terracottatech.ehcache.clustered.client.config.builders.EnterpriseServerSideConfigurationBuilder");


      Class<?> enterpriseClusteringServiceConfigurationBuilderClass = loadClass("com.terracottatech.ehcache.clustered.client.config.builders.EnterpriseClusteringServiceConfigurationBuilder");
      Method enterpriseClusterMethod = enterpriseClusteringServiceConfigurationBuilderClass.getMethod("enterpriseCluster", URI.class);

      Method autoCreateMethod = enterpriseClusteringServiceConfigurationBuilderClass.getMethod("autoCreate");
      Method defaultServerResourceMethod = enterpriseServerSideConfigurationBuilderClass.getMethod("defaultServerResource", String.class);
      Method resourcePoolMethod4 = enterpriseServerSideConfigurationBuilderClass.getMethod("resourcePool", String.class, long.class, memoryUnitClass, String.class);
      Method resourcePoolMethod3 = enterpriseServerSideConfigurationBuilderClass.getMethod("resourcePool", String.class, long.class, memoryUnitClass);
      Method restartableMethod = enterpriseServerSideConfigurationBuilderClass.getMethod("restartable", String.class);


      Object enterpriseClusteringServiceConfigurationBuilder = enterpriseClusterMethod.invoke(null, clusterUri.resolve("/SEFor" + cmName));
      Object enterpriseServerSideConfigurationBuilder = autoCreateMethod.invoke(enterpriseClusteringServiceConfigurationBuilder);
      enterpriseServerSideConfigurationBuilder = defaultServerResourceMethod.invoke(enterpriseServerSideConfigurationBuilder, "primary-server-resource");
      enterpriseServerSideConfigurationBuilder = resourcePoolMethod4.invoke(enterpriseServerSideConfigurationBuilder, "resource-pool-a", 128L, mB, "secondary-server-resource");
      enterpriseServerSideConfigurationBuilder = resourcePoolMethod3.invoke(enterpriseServerSideConfigurationBuilder, "resource-pool-b", 64L, mB);
      return restartableMethod.invoke(enterpriseServerSideConfigurationBuilder, "dataroot");
    } else {

      Class<?> serverSideConfigurationBuilderClass = loadClass("org.ehcache.clustered.client.config.builders.ServerSideConfigurationBuilder");
      Class<?> clusteringServiceConfigurationBuilderClass = loadClass("org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder");
      Method enterpriseClusterMethod = clusteringServiceConfigurationBuilderClass.getMethod("cluster", URI.class);

      Method autoCreateMethod = clusteringServiceConfigurationBuilderClass.getMethod("autoCreate");
      Method defaultServerResourceMethod = serverSideConfigurationBuilderClass.getMethod("defaultServerResource", String.class);
      Method resourcePoolMethod4 = serverSideConfigurationBuilderClass.getMethod("resourcePool", String.class, long.class, memoryUnitClass, String.class);
      Method resourcePoolMethod3 = serverSideConfigurationBuilderClass.getMethod("resourcePool", String.class, long.class, memoryUnitClass);


      Object enterpriseClusteringServiceConfigurationBuilder = enterpriseClusterMethod.invoke(null, clusterUri.resolve("/SEFor" + cmName));
      Object enterpriseServerSideConfigurationBuilder = autoCreateMethod.invoke(enterpriseClusteringServiceConfigurationBuilder);
      enterpriseServerSideConfigurationBuilder = defaultServerResourceMethod.invoke(enterpriseServerSideConfigurationBuilder, "primary-server-resource");
      enterpriseServerSideConfigurationBuilder = resourcePoolMethod4.invoke(enterpriseServerSideConfigurationBuilder, "resource-pool-a", 128L, mB, "secondary-server-resource");
      enterpriseServerSideConfigurationBuilder = resourcePoolMethod3.invoke(enterpriseServerSideConfigurationBuilder, "resource-pool-b", 64L, mB);
      return enterpriseServerSideConfigurationBuilder;
    }


  }

  private Object defaultCacheConfigurationHeapOffHeapDedicatedClustered() throws Exception {
    Class<?> cacheConfigurationBuilderClass = loadClass("org.ehcache.config.builders.CacheConfigurationBuilder");
    Class<?> builderClass = loadClass("org.ehcache.config.Builder");
    Method newCacheConfigurationBuilderMethod = cacheConfigurationBuilderClass.getMethod("newCacheConfigurationBuilder", Class.class, Class.class, builderClass);
    Object resourcePoolsBuilder = defaultCacheConfigurationCacheResourcePoolBuilder(kitAwareClassLoaderDelegator.isEEKit());
    Object cacheConfigurationBuilder = newCacheConfigurationBuilderMethod.invoke(null, String.class, String.class, resourcePoolsBuilder);
    Method buildMethod = cacheConfigurationBuilderClass.getMethod("build");
    Object cacheConfiguration = buildMethod.invoke(cacheConfigurationBuilder);
    return cacheConfiguration;
  }

  private Object defaultCacheConfigurationCacheResourcePoolBuilder(boolean eeKit) throws Exception {
    Class<?> resourcePoolsBuilderClass = loadClass("org.ehcache.config.builders.ResourcePoolsBuilder");
    Method heapMethod = resourcePoolsBuilderClass.getMethod("heap", long.class);
    Object resourcePoolsBuilder = heapMethod.invoke(null, 1000L);
    Class<?> memoryUnitClass = loadClass("org.ehcache.config.units.MemoryUnit");
    Method valueOfMethod = memoryUnitClass.getMethod("valueOf", String.class);
    Object mB = valueOfMethod.invoke(null, "MB");
    Method offHeapMethod = resourcePoolsBuilderClass.getMethod("offheap", long.class, memoryUnitClass);
    resourcePoolsBuilder = offHeapMethod.invoke(resourcePoolsBuilder, 10L, mB);
    Class<?> resourcePoolClass = loadClass("org.ehcache.config.ResourcePool");
    Method withMethod = resourcePoolsBuilderClass.getMethod("with", resourcePoolClass);

    Object clusteredDedicatedResourcePoolImpl;
    if (eeKit) {
      Class<?> enterpriseClusteredDedicatedResourcePoolImplClass = loadClass("com.terracottatech.ehcache.clustered.client.internal.config.EnterpriseClusteredDedicatedResourcePoolImpl");
      Constructor<?> enterpriseClusteredDedicatedResourcePoolImplConstructor = enterpriseClusteredDedicatedResourcePoolImplClass.getConstructor(String.class, long.class, memoryUnitClass);
      clusteredDedicatedResourcePoolImpl = enterpriseClusteredDedicatedResourcePoolImplConstructor.newInstance("primary-server-resource", 20, mB);
    } else {
      Class<?> clusteredDedicatedResourcePoolImplClass = loadClass("org.ehcache.clustered.client.internal.config.DedicatedClusteredResourcePoolImpl");
      Constructor<?> enterpriseClusteredDedicatedResourcePoolImplConstructor = clusteredDedicatedResourcePoolImplClass.getConstructor(String.class, long.class, memoryUnitClass);
      clusteredDedicatedResourcePoolImpl = enterpriseClusteredDedicatedResourcePoolImplConstructor.newInstance("primary-server-resource", 20, mB);
    }

    Object resourcePoolBuilder = withMethod.invoke(resourcePoolsBuilder, clusteredDedicatedResourcePoolImpl);
    return resourcePoolBuilder;
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
  public boolean isCacheManagerAlive() {
    return !getStatus().equals("UNINITIALIZED") && !getStatus().equals(NO_CACHE_MANAGER);
  }

  @Override
  public String getStatus() {

    if (cacheManager == null) {
      return NO_CACHE_MANAGER;
    }

    try {
      Thread.currentThread().setContextClassLoader(kitAwareClassLoaderDelegator.getUrlClassLoader());
      Method getStatusMethod = ehCacheManagerClass.getMethod("getStatus");
      Object status = getStatusMethod.invoke(cacheManager);
      return status.toString();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
