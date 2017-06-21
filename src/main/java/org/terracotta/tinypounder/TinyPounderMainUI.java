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

import com.vaadin.annotations.StyleSheet;
import com.vaadin.data.provider.ListDataProvider;
import com.vaadin.server.Page;
import com.vaadin.server.VaadinRequest;
import com.vaadin.spring.annotation.SpringUI;
import com.vaadin.ui.Alignment;
import com.vaadin.ui.Button;
import com.vaadin.ui.ComboBox;
import com.vaadin.ui.Component;
import com.vaadin.ui.HorizontalLayout;
import com.vaadin.ui.Label;
import com.vaadin.ui.Notification;
import com.vaadin.ui.PopupView;
import com.vaadin.ui.Slider;
import com.vaadin.ui.TextArea;
import com.vaadin.ui.TextField;
import com.vaadin.ui.UI;
import com.vaadin.ui.VerticalLayout;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * The UI class
 * <p>
 * This class should never import anything from the Ehcache API
 */
//@Theme("tinytheme")
@StyleSheet("tinypounder.css")
@SpringUI
public class TinyPounderMainUI extends UI {


  @Autowired
  private CacheManagerBusiness cacheManagerBusiness;

  @Autowired
  private KitAwareClassLoaderDelegator kitAwareClassLoaderDelegator;

  private VerticalLayout mainLayout;
  private VerticalLayout cacheControls;
  private VerticalLayout cacheManagerControls;

  @Override
  protected void init(VaadinRequest vaadinRequest) {
    setupLayout();
    addKitControls();
  }

  private void addCacheControls() {

    List<String> cacheNames = new ArrayList<>(cacheManagerBusiness.retrieveCacheNames());
    cacheControls = new VerticalLayout();
    VerticalLayout cacheList = new VerticalLayout();
    HorizontalLayout cacheCreation = new HorizontalLayout();
    cacheControls.addComponentsAndExpand(cacheList, cacheCreation);

    TextField cacheNameField = new TextField();
    cacheNameField.setPlaceholder("a cache name");
    cacheNameField.addStyleName("align-bottom");
    cacheCreation.addComponent(cacheNameField);

    List<Long> onHeapValues = Arrays.asList(0L, 1L, 10L, 100L, 1000L);
    ComboBox<Long> onHeapSizeComboBox = new ComboBox<>("OnHeap size", onHeapValues);
    onHeapSizeComboBox.addStyleName("small-combo");
    onHeapSizeComboBox.setEmptySelectionAllowed(false);
    onHeapSizeComboBox.setValue(onHeapValues.get(3));
    cacheCreation.addComponent(onHeapSizeComboBox);

    List<String> onHeapUnitValues = Arrays.asList("ENTRIES", "KB", "MB", "GB");
    ComboBox<String> onHeapUnitComboBox = new ComboBox<>("OnHeap unit", onHeapUnitValues);
    onHeapUnitComboBox.setValue(onHeapUnitValues.get(0));
    cacheCreation.addComponent(onHeapUnitComboBox);

    List<Long> offHeapValues = Arrays.asList(0L, 1L, 10L, 100L, 1000L);
    ComboBox<Long> offHeapSizeComboBox = new ComboBox<>("Offheap size", offHeapValues);
    offHeapSizeComboBox.addStyleName("small-combo");
    offHeapSizeComboBox.setEmptySelectionAllowed(false);
    offHeapSizeComboBox.setValue(offHeapValues.get(2));
    cacheCreation.addComponent(offHeapSizeComboBox);

    List<String> offHeapUnitValues = Arrays.asList("KB", "MB", "GB");
    ComboBox<String> offHeapUnitComboBox = new ComboBox<>("OffHeap unit", offHeapUnitValues);
    offHeapUnitComboBox.addStyleName("small-combo");
    offHeapUnitComboBox.setValue(offHeapUnitValues.get(1));
    cacheCreation.addComponent(offHeapUnitComboBox);


    List<Long> diskValues = Arrays.asList(0L, 1L, 10L, 100L, 1000L);
    ComboBox<Long> diskSizeComboBox = new ComboBox<>("Disk size", diskValues);
    diskSizeComboBox.addStyleName("small-combo");
    diskSizeComboBox.setEmptySelectionAllowed(false);
    diskSizeComboBox.setValue(diskValues.get(0));
    cacheCreation.addComponent(diskSizeComboBox);

    List<String> diskUnitValues = Arrays.asList("KB", "MB", "GB");
    ComboBox<String> diskUnitComboBox = new ComboBox<>("Disk unit", diskUnitValues);
    diskUnitComboBox.addStyleName("small-combo");
    diskUnitComboBox.setValue(diskUnitValues.get(1));
    cacheCreation.addComponent(diskUnitComboBox);

    List<String> clusteredValues = Arrays.asList("NONE", "shared", "dedicated 10MB", "dedicated 100MB");
    ComboBox<String> clusteredComboBox = new ComboBox<>("Clustered tier", clusteredValues);
    clusteredComboBox.setEmptySelectionAllowed(false);
    clusteredComboBox.setValue(clusteredValues.get(2));
    cacheCreation.addComponent(clusteredComboBox);


    Button addCacheButton = new Button("Add cache");
    addCacheButton.addStyleName("align-bottom");

    cacheCreation.addComponent(addCacheButton);
    ListDataProvider<String> listDataProvider = new ListDataProvider<>(cacheNames);
    addCacheButton.addClickListener(clickEvent -> {
      try {

        String clusteredComboBoxValue = clusteredComboBox.getValue();
        CacheConfiguration.ClusterTierType clusterTierType = CacheConfiguration.ClusterTierType.NONE;
        int clusteredDedicatedSize = 0;
        String clusteredDedicatedUnit = "MB";
        String clusteredSharedPoolName = null;
        if (clusteredComboBoxValue.equals("shared")) {
          clusterTierType = CacheConfiguration.ClusterTierType.SHARED;
          clusteredSharedPoolName = "resource-pool-a";
        } else if (clusteredComboBoxValue.contains("dedicated")) {
          clusterTierType = CacheConfiguration.ClusterTierType.DEDICATED;
          String fullsize = clusteredComboBoxValue.split(" ")[1];
          clusteredDedicatedSize = Integer.valueOf(fullsize.split("MB")[0]);
        }

        CacheConfiguration cacheConfiguration = new CacheConfiguration(
            onHeapSizeComboBox.getValue(),
            onHeapUnitComboBox.getValue(),
            offHeapSizeComboBox.getValue(),
            offHeapUnitComboBox.getValue(),
            diskSizeComboBox.getValue(),
            diskUnitComboBox.getValue(), clusteredDedicatedSize, clusteredDedicatedUnit, clusteredSharedPoolName, clusterTierType);

        cacheManagerBusiness.createCache(cacheNameField.getValue(), cacheConfiguration);
        cacheNames.add(cacheNameField.getValue());
        refreshStuff(listDataProvider);
        cacheNameField.clear();
        Notification notification = new Notification("Cache added with success !",
            Notification.Type.TRAY_NOTIFICATION);
        notification.setStyleName("warning");
        notification.show(Page.getCurrent());
      } catch (RuntimeException e) {
        displayErrorNotification("Cache could not be added !", ExceptionUtils.getRootCauseMessage(e));
      }
    });

    for (String cacheName : cacheNames) {
      HorizontalLayout cacheInfo = new HorizontalLayout();
      Label cacheNameLabel = new Label(cacheName);

      Slider poundingSlider = new Slider();
      poundingSlider.setCaption("NOT POUNDING");
      poundingSlider.addStyleName("pounding-slider");
      if (cacheManagerBusiness.retrievePoundingIntensity(cacheName) > 0) {
        poundingSlider.setValue((double) cacheManagerBusiness.retrievePoundingIntensity(cacheName));
        updatePoundingCaption(poundingSlider, cacheManagerBusiness.retrievePoundingIntensity(cacheName));
      }
      poundingSlider.setMin(0);
      poundingSlider.setMax(11);
      poundingSlider.addValueChangeListener(event -> {
        int poundingIntensity = event.getValue().intValue();
        cacheManagerBusiness.updatePoundingIntensity(cacheName, poundingIntensity);
        updatePoundingCaption(poundingSlider, poundingIntensity);
      });


      Button removeCacheButton = new Button("Remove cache");
      removeCacheButton.addClickListener(event -> {
        try {
          cacheManagerBusiness.removeCache(cacheName);
          cacheNames.remove(cacheName);
          refreshStuff(listDataProvider);
          Notification notification = new Notification("Cache removed with success !",
              Notification.Type.TRAY_NOTIFICATION);
          notification.setStyleName("warning");
          notification.show(Page.getCurrent());
        } catch (RuntimeException e) {
          displayErrorNotification("Cache could not be removed !", ExceptionUtils.getRootCauseMessage(e));
          refreshStuff(listDataProvider);
        }
      });

      Button destroyCacheButton = new Button("Destroy cache");
      destroyCacheButton.addClickListener(event -> {
        try {
          cacheManagerBusiness.destroyCache(cacheName);
          cacheNames.remove(cacheName);
          refreshStuff(listDataProvider);
          Notification notification = new Notification("Cache destroyed with success !",
              Notification.Type.TRAY_NOTIFICATION);
          notification.setStyleName("warning");
          notification.show(Page.getCurrent());
        } catch (Exception e) {
          displayErrorNotification("Cache could not be destroyed !", ExceptionUtils.getRootCauseMessage(e));
          refreshStuff(listDataProvider);
        }
      });
      cacheInfo.addComponentsAndExpand(cacheNameLabel, poundingSlider, removeCacheButton, destroyCacheButton);
      cacheList.addComponent(cacheInfo);
    }


    mainLayout.addComponent(cacheControls);

  }

  private void updatePoundingCaption(Slider poundingSlider, int poundingIntensity) {
    if (poundingIntensity == 0) {
      poundingSlider.setCaption("NOT POUNDING");
    } else if (poundingIntensity > 0 && poundingIntensity < 11) {
      poundingSlider.setCaption("POUNDING");
    } else {
      poundingSlider.setCaption("POUNDING HARD");
    }
  }

  private void displayErrorNotification(String caption, String message) {
    Notification notification = new Notification(caption,
        message,
        Notification.Type.TRAY_NOTIFICATION);
    notification.setStyleName("error");
    notification.show(Page.getCurrent());
  }

  private void setupLayout() {
    mainLayout = new VerticalLayout();
    mainLayout.setDefaultComponentAlignment(Alignment.MIDDLE_CENTER);
    setContent(mainLayout);
//    Label header = new Label("Welcome to the tiny pounder");
//    header.addStyleName(ValoTheme.LABEL_H1);
//    mainLayout.addComponent(header);

  }

  private void addCacheManagerControls() {

    cacheManagerControls = new VerticalLayout();
    HorizontalLayout mainCacheManagerControls = new HorizontalLayout();


    TextArea cacheManagerConfigTextArea = new TextArea();
    cacheManagerConfigTextArea.setValue(cacheManagerBusiness.isCacheManagerAlive() ? cacheManagerBusiness.retrieveHumanReadableConfiguration() : "CacheManager configuration will be displayed here");
    cacheManagerConfigTextArea.setWidth(400, Unit.PERCENTAGE);
    cacheManagerConfigTextArea.setRows(15);
    CacheManagerConfigurationPopup cacheManagerConfigurationPopup = new CacheManagerConfigurationPopup(cacheManagerConfigTextArea);
    PopupView popupView = new PopupView(cacheManagerConfigurationPopup);
    popupView.setWidth("600px");
    popupView.setSizeFull();


    Label statusLabel = new Label();
    statusLabel.setValue(cacheManagerBusiness.getStatus());

    TextField terracottaUrlField = new TextField();
    terracottaUrlField.setValue("localhost:9510");

    TextField cmNameField = new TextField();
    cmNameField.setValue("TinyPounderCM");

    TextField diskPersistenceLocationField = new TextField();
    diskPersistenceLocationField.setValue("tinyPounderDiskPersistence");

    Button createCacheManager = new Button("Initialize CacheManager");
    createCacheManager.addClickListener(event -> {
      try {
        cacheManagerBusiness.initializeCacheManager(terracottaUrlField.getValue(), cmNameField.getValue(), diskPersistenceLocationField.getValue());
        cacheManagerConfigTextArea.setValue(cacheManagerBusiness.retrieveHumanReadableConfiguration());
        statusLabel.setValue(cacheManagerBusiness.getStatus());
        refreshCacheControls();
      } catch (Exception e) {
        displayErrorNotification("CacheManager could not be initialized!", ExceptionUtils.getRootCauseMessage(e));
      }
    });

    Button closeCacheManager = new Button("Close CacheManager");
    closeCacheManager.addClickListener(event -> {
      try {
        cacheManagerBusiness.close();
        refreshCacheControls();
        refreshCacheManagerControls();
      } catch (Exception e) {
        displayErrorNotification("CacheManager could not be closed!", ExceptionUtils.getRootCauseMessage(e));
      }
    });
    Button destroyCacheManager = new Button("Destroy CacheManager");
    destroyCacheManager.addClickListener(event -> {
      try {
        cacheManagerBusiness.destroy();
        refreshCacheControls();
        refreshCacheManagerControls();
        Notification notification = new Notification("CacheManager destroyed with success !",
            Notification.Type.TRAY_NOTIFICATION);
        notification.setStyleName("warning");
        notification.show(Page.getCurrent());
      } catch (Exception e) {
        displayErrorNotification("CacheManager could not be destroyed!", ExceptionUtils.getRootCauseMessage(e));
      }
    });

    mainCacheManagerControls.addComponentsAndExpand(statusLabel, terracottaUrlField, cmNameField, diskPersistenceLocationField, createCacheManager, closeCacheManager, destroyCacheManager);
    cacheManagerControls.addComponentsAndExpand(mainCacheManagerControls, popupView);
    mainLayout.addComponent(cacheManagerControls);

    if (cacheManagerBusiness.isCacheManagerAlive()) {
      addCacheControls();
    }

  }

  private void addKitControls() {

    VerticalLayout kitControlsLayout = new VerticalLayout();
    HorizontalLayout kitPathLayout = new HorizontalLayout();

    Label info = new Label("Using " + (kitAwareClassLoaderDelegator.isEEKit() ? "Enterprise" : "Open source") + " kit at : " + kitAwareClassLoaderDelegator.getKitPath());
    TextField kitPath = new TextField();
    kitPath.setWidth("100%");
    kitPath.setValue(kitAwareClassLoaderDelegator.getKitPath() != null ? kitAwareClassLoaderDelegator.getKitPath() : "");
    Button changePathButton = new Button("Update kit path");
    changePathButton.setEnabled(false);
    kitPath.addValueChangeListener(event -> changePathButton.setEnabled(true));
    kitPathLayout.addComponent(kitPath);
    kitPathLayout.addComponent(changePathButton);

    changePathButton.addClickListener(event -> {
      try {
        kitAwareClassLoaderDelegator.setKitPathAndUpdate(kitPath.getValue());
        info.setValue("Using " + (kitAwareClassLoaderDelegator.isEEKit() ? "Enterprise" : "Open source") + " kit at : " + kitAwareClassLoaderDelegator.getKitPath());
        refreshCacheManagerControls();
      } catch (Exception e) {
        displayErrorNotification("Kit path could not update !", ExceptionUtils.getRootCauseMessage(e));

      }
    });

    kitControlsLayout.addComponent(info);
    kitControlsLayout.addComponent(kitPathLayout);
    mainLayout.addComponent(kitControlsLayout);

    if (kitAwareClassLoaderDelegator.isProperlyInitialized()) {
      addCacheManagerControls();
    }

  }

  private void refreshCacheControls() {
    if (cacheControls != null) {
      mainLayout.removeComponent(cacheControls);
    }
    if (cacheManagerBusiness.isCacheManagerAlive()) {
      addCacheControls();
    }

  }

  private void refreshCacheManagerControls() {
    if (kitAwareClassLoaderDelegator.isProperlyInitialized()) {
      if (cacheManagerControls != null) {
        mainLayout.removeComponent(cacheManagerControls);
        if (cacheControls != null) {
          mainLayout.removeComponent(cacheControls);
        }

      }
      addCacheManagerControls();
    }
  }

  private void refreshStuff(ListDataProvider<String> listDataProvider) {
    listDataProvider.refreshAll();
    refreshCacheManagerControls();
  }

  // Create a dynamically updating content for the popup
  private static class CacheManagerConfigurationPopup implements PopupView.Content {
    private final TextArea textArea;

    public CacheManagerConfigurationPopup(TextArea textArea) {
      this.textArea = textArea;
    }

    @Override
    public final Component getPopupComponent() {
      return textArea;
    }

    @Override
    public final String getMinimizedValueAsHTML() {
      return "CacheManager full configuration";
    }
  }


}