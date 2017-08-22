package org.terracotta.tinypounder;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigInteger;
import java.net.URI;
import java.util.Collection;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Service
public class DatasetManagerBusinessReflectionImpl {

  private final ConcurrentMap<String, Integer> poundingMap = new ConcurrentHashMap<>();
  private static final String NO_DATASET_MANAGER = "NO DATASET MANAGER";
  private final KitAwareClassLoaderDelegator kitAwareClassLoaderDelegator;
  private Class<?> datasetManagerClass;
  private Object datasetManager;
  private final Map<String, Map<String, Object>> datasetInstancesByDatasetName = new TreeMap<>();
  private final static String DATASET_INSTANCE_PATTERN = "^(.*)-[0-9]*$";
  private Random random = new Random();
  private Object stringCellDefinition;
  private Class<?> cellDefinitionClass;
  private Object typeString;
  private Class<?> typeClass;

  @Autowired
  public DatasetManagerBusinessReflectionImpl(KitAwareClassLoaderDelegator kitAwareClassLoaderDelegator, ScheduledExecutorService poundingScheduler) throws Exception {
    this.kitAwareClassLoaderDelegator = kitAwareClassLoaderDelegator;
    poundingScheduler.scheduleAtFixedRate(() -> poundingMap.entrySet().parallelStream().forEach(entryConsumer -> {
      try {
        Object datasetInstance = retrieveDatasetInstance(entryConsumer.getKey());
        if (datasetInstance != null && entryConsumer.getValue() > 0) {
          pound(datasetInstance, entryConsumer.getValue());
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }), 5000, 100, TimeUnit.MILLISECONDS);
  }

  private void pound(Object datasetInstance, Integer intensity) {
    IntStream.range(0, intensity).forEach(value -> {
      insert(datasetInstance, Long.toString(ThreadLocalRandom.current().nextLong(0, intensity * 1000)), longString(intensity));
      update(datasetInstance, Long.toString(ThreadLocalRandom.current().nextLong(0, intensity * 1000)), longString(intensity));
      delete(datasetInstance, Long.toString(ThreadLocalRandom.current().nextLong(0, intensity * 1000)));
      stream(datasetInstance);
      retrieve(datasetInstance, Long.toString(ThreadLocalRandom.current().nextLong(0, intensity * 1000)));
    });
    try {
      generateRandomFailure(datasetInstance);
    } catch (Exception e) {
      // of course, we were generating a failure !
    }
  }

  private void generateRandomFailure(Object datasetInstance) {
    int nextInt = ThreadLocalRandom.current().nextInt(0, 5);
    switch (nextInt) {
      case 0:
        insert(datasetInstance, null, longString(0));
        break;
      case 1:
        update(datasetInstance, null, longString(0));
        break;
      case 2:
        retrieve(datasetInstance, null);
        break;
      case 3:
        delete(datasetInstance, null);
        break;
      case 4:
        recordsFailure(datasetInstance);
        break;
    }

  }

  private Object swapUnderlying(Object datasetReader, Object datasetReaderToReplaceWith) {
    // special thanks to Henri ;-)  !
    try {
      Thread.currentThread().setContextClassLoader(kitAwareClassLoaderDelegator.getUrlClassLoader());
      Class<?> statisticsDatasetReaderClass = loadClass("com.terracottatech.store.statistics.StatisticsDatasetReader");
      Field underlying = statisticsDatasetReaderClass.getDeclaredField("underlying");
      underlying.setAccessible(true);
      Object current = underlying.get(datasetReader);
      underlying.set(datasetReader, datasetReaderToReplaceWith);
      return current;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }


  private void recordsFailure(Object datasetInstance) {
    // dataset.records()
    try {
      Thread.currentThread().setContextClassLoader(kitAwareClassLoaderDelegator.getUrlClassLoader());
      Object datasetWriterReader = retrieveDatasetWriterReader(datasetInstance);
      Object formerUnderlying = swapUnderlying(datasetWriterReader, null);
      try {
        Class<?> datasetWriterReaderClass = loadClass("com.terracottatech.store.DatasetWriterReader");
        Method recordsMethod = datasetWriterReaderClass.getMethod("records");
        recordsMethod.invoke(datasetWriterReader);
      } catch (Exception e) {
        // well, playing with fire, you get burnt ! we need to reswap now !
      }
      swapUnderlying(datasetWriterReader, formerUnderlying);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void stream(Object datasetInstance) {
    // dataset.records().filter(stringCellDefinition.value().is("0)).count()
    try {
      Thread.currentThread().setContextClassLoader(kitAwareClassLoaderDelegator.getUrlClassLoader());
      Object datasetWriterReader = retrieveDatasetWriterReader(datasetInstance);
      Class<?> datasetWriterReaderClass = loadClass("com.terracottatech.store.DatasetWriterReader");
      Method recordsMethod = datasetWriterReaderClass.getMethod("records");
      Object records = recordsMethod.invoke(datasetWriterReader);

      Class<?> mutableRecordStreamClass = loadClass("com.terracottatech.store.stream.MutableRecordStream");
      Method filterMethod = mutableRecordStreamClass.getMethod("filter", Predicate.class);
      Method countMethod = mutableRecordStreamClass.getMethod("count");

      Method valueMethod = cellDefinitionClass.getMethod("value");
      Object value = valueMethod.invoke(stringCellDefinition);

      Class<?> buildableOptionalFunctionClass = loadClass("com.terracottatech.store.function.BuildableStringOptionalFunction");
      Method isMethod = buildableOptionalFunctionClass.getMethod("is", Object.class);
      Object isPredicate = isMethod.invoke(value, "0");

      Object filter = filterMethod.invoke(records, isPredicate);
      countMethod.invoke(filter);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void delete(Object datasetInstance, String key) {
    try {
      Thread.currentThread().setContextClassLoader(kitAwareClassLoaderDelegator.getUrlClassLoader());
      Object datasetWriterReader = retrieveDatasetWriterReader(datasetInstance);
      Class<?> datasetWriterReaderClass = loadClass("com.terracottatech.store.DatasetWriterReader");
      Method deleteMethod = datasetWriterReaderClass.getMethod("delete", Comparable.class);
      deleteMethod.invoke(datasetWriterReader, key);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void update(Object datasetInstance, String key, String value) {
    try {
      Thread.currentThread().setContextClassLoader(kitAwareClassLoaderDelegator.getUrlClassLoader());
      Object datasetWriterReader = retrieveDatasetWriterReader(datasetInstance);
      Class<?> datasetWriterReaderClass = loadClass("com.terracottatech.store.DatasetWriterReader");

      Class<?> updateOperationClass = loadClass("com.terracottatech.store.UpdateOperation");
      Method writeMethod = updateOperationClass.getMethod("write", cellDefinitionClass, Object.class);
      Object writeOperation = writeMethod.invoke(null, stringCellDefinition, value);

      Method updateMethod = datasetWriterReaderClass.getMethod("update", Comparable.class, updateOperationClass);

      updateMethod.invoke(datasetWriterReader, key, writeOperation);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void retrieve(Object datasetInstance, String key) {
    try {
      Thread.currentThread().setContextClassLoader(kitAwareClassLoaderDelegator.getUrlClassLoader());
      Object datasetWriterReader = retrieveDatasetWriterReader(datasetInstance);
      Class<?> datasetWriterReaderClass = loadClass("com.terracottatech.store.DatasetWriterReader");
      Method getMethod = datasetWriterReaderClass.getMethod("get", Comparable.class);
      getMethod.invoke(datasetWriterReader, key);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private Object retrieveDatasetWriterReader(Object datasetInstance) throws Exception {
    Class<?> internalDatasetClass = loadClass("com.terracottatech.store.internal.InternalDataset");


    Class<?> readVisibilityClas = loadClass("com.terracottatech.store.setting.ReadVisibility");
    Class<?> readSettingsClass = loadClass("com.terracottatech.store.setting.ReadSettings");

    Class<?> writeVisibilityClas = loadClass("com.terracottatech.store.setting.WriteVisibility");
    Class<?> writeSettingsClass = loadClass("com.terracottatech.store.setting.WriteSettings");


    Field definitiveField = readVisibilityClas.getDeclaredField("DEFINITIVE");
    Object definitiveReadVisibility = definitiveField.get(null);
    Method asReadSettingsMethod = readVisibilityClas.getMethod("asReadSettings");
    Object definitiveReadVisibilityAsReadSettings = asReadSettingsMethod.invoke(definitiveReadVisibility);


    Field immediateField = writeVisibilityClas.getDeclaredField("IMMEDIATE");
    Object immediateWriteVisibility = immediateField.get(null);
    Method asWriteSettingsMethod = writeVisibilityClas.getMethod("asWriteSettings");
    Object immediateWriteVisibilityAsWriteSettings = asWriteSettingsMethod.invoke(immediateWriteVisibility);

    Method writerReader = internalDatasetClass.getMethod("writerReader", readSettingsClass, writeSettingsClass);

    return writerReader.invoke(datasetInstance, definitiveReadVisibilityAsReadSettings, immediateWriteVisibilityAsWriteSettings);
  }

  private void insert(Object datasetInstance, String key, String value) {
    try {
      Thread.currentThread().setContextClassLoader(kitAwareClassLoaderDelegator.getUrlClassLoader());
      Object datasetWriterReader = retrieveDatasetWriterReader(datasetInstance);

      Class<?> datasetWriterReaderClass = loadClass("com.terracottatech.store.DatasetWriterReader");

      Class<?> cellClass = loadClass("com.terracottatech.store.Cell");
      Class cellClasses = Array.newInstance(cellClass, 0).getClass();
      Method addMethod = datasetWriterReaderClass.getMethod("add", Comparable.class, cellClasses);

      Object[] cells;
      if (key == null || Long.parseLong(key) % 2 == 0) {
        cells = generateOneCell(value, cellClass);
      } else {
        cells = generateTwoCells(value, cellClass);
      }

      addMethod.invoke(datasetWriterReader, key, cells);

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private Object[] generateTwoCells(String value, Class<?> cellClass) throws Exception {
    Method newCellMethod = cellDefinitionClass.getMethod("newCell", Object.class);
    Object stringCellValue = newCellMethod.invoke(stringCellDefinition, value);

    Method defineBytesMethod = cellDefinitionClass.getMethod("defineBytes", String.class);
    Object bytesCellDefinition = defineBytesMethod.invoke(null, "myBytesCell");
    Object bytesCellValue = newCellMethod.invoke(bytesCellDefinition, value.getBytes());

    Object[] cells = (Object[]) Array.newInstance(cellClass, 2);
    cells[0] = stringCellValue;
    cells[1] = bytesCellValue;
    return cells;
  }

  private Object[] generateOneCell(String value, Class<?> cellClass) throws Exception {
    Class<?> cellDefinitionClass = loadClass("com.terracottatech.store.definition.CellDefinition");
    Method newCellMethod = cellDefinitionClass.getMethod("newCell", Object.class);
    Method defineBytesMethod = cellDefinitionClass.getMethod("defineBytes", String.class);
    Object bytesCellDefinition = defineBytesMethod.invoke(null, "myBytesCell");
    Object bytesCellValue = newCellMethod.invoke(bytesCellDefinition, value.getBytes());

    Object[] cells = (Object[]) Array.newInstance(cellClass, 1);
    cells[0] = bytesCellValue;
    return cells;
  }

  private String longString(Integer intensity) {
    return new BigInteger(intensity * 10, random).toString(16);
  }

  public void updatePoundingIntensity(String datasetInstanceName, int poundingIntensity) {
    poundingMap.put(datasetInstanceName, poundingIntensity);
  }

  public int retrievePoundingIntensity(String datasetInstanceName) {
    return poundingMap.getOrDefault(datasetInstanceName, 0);
  }

  private Object retrieveDatasetInstance(String datasetInstanceName) {
    return datasetInstancesByDatasetName.get(retrieveDatasetName(datasetInstanceName)).get(datasetInstanceName);
  }


  public String getStatus() {
    if (datasetManager == null) {
      return NO_DATASET_MANAGER;
    } else {
      try {
        Thread.currentThread().setContextClassLoader(kitAwareClassLoaderDelegator.getUrlClassLoader());
        // try listing datasets, if we can't, that means it's not available
        Thread.currentThread().setContextClassLoader(kitAwareClassLoaderDelegator.getUrlClassLoader());
        Method listDatasetsMethod = datasetManagerClass.getMethod("listDatasets");
        listDatasetsMethod.invoke(datasetManager);
      } catch (Exception e) {
        return "CLOSED";
      }
    }
    return "AVAILABLE";
  }

  public void initializeDatasetManager(String terracottaServerUrl) {
    try {
      Thread.currentThread().setContextClassLoader(kitAwareClassLoaderDelegator.getUrlClassLoader());
      initCommonObjectsAndClasses();
      if (terracottaServerUrl != null) {
        URI clusterUri = URI.create("terracotta://" + terracottaServerUrl);

        // DatasetManager.clustered(URI.create(uri)).build()
        datasetManagerClass = loadClass("com.terracottatech.store.manager.DatasetManager");
        Method clusteredMethod = datasetManagerClass.getMethod("clustered", URI.class);
        Object clusteredDatasetManagerBuilder = clusteredMethod.invoke(null, clusterUri);

        Class<?> clusteredDatasetManagerBuilderClass = loadClass("com.terracottatech.store.client.builder.datasetmanager.clustered.ClusteredDatasetManagerBuilderImpl");
        Method buildMethod = clusteredDatasetManagerBuilderClass.getMethod("build");
        datasetManager = buildMethod.invoke(clusteredDatasetManagerBuilder);
      } else {
        datasetManager = null;
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void initCommonObjectsAndClasses() throws Exception {
    typeClass = loadClass("com.terracottatech.store.Type");
    Field typeStringField = typeClass.getDeclaredField("STRING");
    typeString = typeStringField.get(null);

    cellDefinitionClass = loadClass("com.terracottatech.store.definition.CellDefinition");
    Method defineStringMethod = cellDefinitionClass.getMethod("defineString", String.class);
    stringCellDefinition = defineStringMethod.invoke(null, "myStringCell");
  }

  public void close() {
    try {
      Thread.currentThread().setContextClassLoader(kitAwareClassLoaderDelegator.getUrlClassLoader());
      Method closeMethod = datasetManagerClass.getMethod("close");
      closeMethod.invoke(datasetManager);
      poundingMap.clear();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public boolean isDatasetManagerAlive() {
    return !getStatus().equals("CLOSED") && !getStatus().equals(NO_DATASET_MANAGER);
  }

  private Class loadClass(String className) throws ClassNotFoundException {
    return kitAwareClassLoaderDelegator.getUrlClassLoader().loadClass(className);
  }

  public Collection<String> retrieveDatasetNames() {
    try {
      Thread.currentThread().setContextClassLoader(kitAwareClassLoaderDelegator.getUrlClassLoader());
      Method listDatasetsMethod = datasetManagerClass.getMethod("listDatasets");
      Map<String, Object> datasetsByName = (Map<String, Object>) listDatasetsMethod.invoke(datasetManager);
      return datasetsByName.keySet();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void createDataset(String datasetName, DatasetConfiguration datasetConfiguration) {
    try {
      Thread.currentThread().setContextClassLoader(kitAwareClassLoaderDelegator.getUrlClassLoader());
      Class<?> indexSettingsClass = loadClass("com.terracottatech.store.indexing.IndexSettings");
      Field btreeField = indexSettingsClass.getDeclaredField("BTREE");
      Object btree = btreeField.get(null);
      Method datasetConfigurationMethod = datasetManagerClass.getMethod("datasetConfiguration");
      Object datasetConfigurationBuilder = datasetConfigurationMethod.invoke(datasetManager);

      Class<?> datasetConfigurationClass = loadClass("com.terracottatech.store.configuration.DatasetConfiguration");
      Class<?> datasetConfigurationBuilderClass = loadClass("com.terracottatech.store.configuration.DatasetConfigurationBuilder");


      Method offHeapMethod = datasetConfigurationBuilderClass.getMethod("offheap", String.class);
      Method diskMethod = datasetConfigurationBuilderClass.getMethod("disk", String.class);
      Method buildMethod = datasetConfigurationBuilderClass.getMethod("build");

      if (datasetConfiguration.useIndex()) {
        Method indexMethod = datasetConfigurationBuilderClass.getMethod("index", cellDefinitionClass, indexSettingsClass);
        datasetConfigurationBuilder = indexMethod.invoke(datasetConfigurationBuilder, stringCellDefinition, btree);
      }
      if (datasetConfiguration.getOffheapResourceName() != null) {
        datasetConfigurationBuilder = offHeapMethod.invoke(datasetConfigurationBuilder, datasetConfiguration.getOffheapResourceName());
      }
      if (datasetConfiguration.getDiskResourceName() != null) {
        datasetConfigurationBuilder = diskMethod.invoke(datasetConfigurationBuilder, datasetConfiguration.getDiskResourceName());
      }

      Object datasetConfigurationBuilt = buildMethod.invoke(datasetConfigurationBuilder);

      Method createDatasetMethod = datasetManagerClass.getMethod("createDataset", String.class, typeClass, datasetConfigurationClass);
      Object datasetInstance = createDatasetMethod.invoke(datasetManager, datasetName, typeString, datasetConfigurationBuilt);

      String instanceName = getInstanceName(datasetInstance);
      Map<String, Object> instancesByName = new TreeMap<>();
      instancesByName.put(instanceName, datasetInstance);
      datasetInstancesByDatasetName.put(datasetName, instancesByName);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private String getInstanceName(Object datasetInstance) throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
    Class<?> manageableDatasetClass = loadClass("com.terracottatech.store.internal.InternalDataset");
    Method getStatisticsMethod = manageableDatasetClass.getMethod("getStatistics");
    Object datasetStatistics = getStatisticsMethod.invoke(datasetInstance);
    Class<?> datasetStatisticsClass = loadClass("com.terracottatech.store.statistics.DatasetStatistics");
    Method getInstanceNameMethod = datasetStatisticsClass.getMethod("getInstanceName");
    return (String) getInstanceNameMethod.invoke(datasetStatistics);
  }

  public void destroyDataset(String datasetName) {
    try {
      Thread.currentThread().setContextClassLoader(kitAwareClassLoaderDelegator.getUrlClassLoader());
      Method destroyDatasetMethod = datasetManagerClass.getMethod("destroyDataset", String.class);
      destroyDatasetMethod.invoke(datasetManager, datasetName);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public Set<String> getDatasetInstanceNames(String datasetName) {
    try {
      Thread.currentThread().setContextClassLoader(kitAwareClassLoaderDelegator.getUrlClassLoader());

      Class<?> internalDatasetManagerClass = loadClass("com.terracottatech.store.internal.InternalDatasetManager");
      Class<?> statisticsServiceClass = loadClass("com.terracottatech.store.statistics.StatisticsService");
      Method getStatisticsServiceMethod = internalDatasetManagerClass.getMethod("getStatisticsService");
      Method getDatasetInstanceNamesMethod = statisticsServiceClass.getMethod("getDatasetInstanceNames");

      Object statisticsService = getStatisticsServiceMethod.invoke(datasetManager);
      Collection<String> datasetInstanceNames = (Collection<String>) getDatasetInstanceNamesMethod.invoke(statisticsService);
      return datasetInstanceNames.stream().filter(datasetInstanceName -> {
        String retrievedDatasetName = retrieveDatasetName(datasetInstanceName);
        return datasetName.equals(retrievedDatasetName);
      }).collect(Collectors.toSet());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private String retrieveDatasetName(String datasetInstanceName) {
    Pattern pattern = Pattern.compile(DATASET_INSTANCE_PATTERN);
    Matcher matcher = pattern.matcher(datasetInstanceName);
    if (matcher.matches() && (matcher.groupCount() > 0)) {
      return matcher.group(1);
    }
    return "";
  }

  public void closeDatasetInstance(String datasetName, String instanceName) {
    poundingMap.remove(instanceName);
    Object datasetInstance = datasetInstancesByDatasetName.get(datasetName).get(instanceName);
    try {
      Thread.currentThread().setContextClassLoader(kitAwareClassLoaderDelegator.getUrlClassLoader());
      Class<?> datasetClass = loadClass("com.terracottatech.store.Dataset");
      Method closeMethod = datasetClass.getMethod("close");
      closeMethod.invoke(datasetInstance);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public String createDatasetInstance(String datasetName) {
    try {
      Thread.currentThread().setContextClassLoader(kitAwareClassLoaderDelegator.getUrlClassLoader());

      Method getDatasetMethod = datasetManagerClass.getMethod("getDataset", String.class, typeClass);
      Object datasetInstance = getDatasetMethod.invoke(datasetManager, datasetName, typeString);
      String instanceName = getInstanceName(datasetInstance);

      datasetInstancesByDatasetName.computeIfAbsent(datasetName, k -> new TreeMap<>());

      datasetInstancesByDatasetName.get(datasetName).put(instanceName, datasetInstance);
      return instanceName;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

  }
}
