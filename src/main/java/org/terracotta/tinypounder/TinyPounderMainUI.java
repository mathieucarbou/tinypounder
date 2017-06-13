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
import com.vaadin.ui.TextArea;
import com.vaadin.ui.TextField;
import com.vaadin.ui.UI;
import com.vaadin.ui.VerticalLayout;
import com.vaadin.ui.themes.ValoTheme;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.ArrayList;
import java.util.List;

/**
 * The UI class
 * <p>
 * This class should never import anything from the Ehcache API
 */
@SpringUI
public class TinyPounderMainUI extends UI {


  @Autowired
  private CacheManagerBusiness cacheManagerBusiness;

  @Autowired
  private KitAwareClassLoaderDelegator kitAwareClassLoaderDelegator;

  private VerticalLayout mainLayout;
  private HorizontalLayout cacheControls;
  private VerticalLayout cacheManagerControls;

  @Override
  protected void init(VaadinRequest vaadinRequest) {
    setupLayout();
    addKitControls();
  }

  private void addCacheControls() {

    List<String> cacheNames = new ArrayList<>(cacheManagerBusiness.retrieveCacheNames());
    cacheControls = new HorizontalLayout();
    cacheControls.setWidth("80%");

    TextField addCacheTextField = new TextField();
    addCacheTextField.setPlaceholder("a cache name");
    cacheControls.addComponent(addCacheTextField);
    Button addCacheButton = new Button("Add cache");
    cacheControls.addComponent(addCacheButton);
    ListDataProvider<String> listDataProvider = new ListDataProvider<>(cacheNames);
    addCacheButton.addClickListener(clickEvent -> {
      try {
        cacheManagerBusiness.createCache(addCacheTextField.getValue());
        cacheNames.add(addCacheTextField.getValue());
        refreshStuff(listDataProvider);
        addCacheTextField.clear();
        Notification notification = new Notification("Cache added with success !",
            Notification.Type.TRAY_NOTIFICATION);
        notification.setStyleName("warning");
        notification.show(Page.getCurrent());
      } catch (RuntimeException e) {
        displayErrorNotification("Cache could not be added !", ExceptionUtils.getRootCauseMessage(e));
      }
    });

    ComboBox<String> removeCacheComboBox = new ComboBox();
    removeCacheComboBox.setDataProvider(listDataProvider);
    removeCacheComboBox.setPlaceholder("Select your cache");
    cacheControls.addComponent(removeCacheComboBox);
    Button removeCacheButton = new Button("Remove cache");
    cacheControls.addComponent(removeCacheButton);
    removeCacheButton.addClickListener(event -> {
      try {
        cacheManagerBusiness.removeCache(removeCacheComboBox.getSelectedItem().get());
        cacheNames.remove(removeCacheComboBox.getSelectedItem().get());
        refreshStuff(listDataProvider);
        removeCacheComboBox.clear();
        Notification notification = new Notification("Cache removed with success !",
            Notification.Type.TRAY_NOTIFICATION);
        notification.setStyleName("warning");
        notification.show(Page.getCurrent());
      } catch (RuntimeException e) {
        displayErrorNotification("Cache could not be removed !", ExceptionUtils.getRootCauseMessage(e));
        refreshStuff(listDataProvider);
      }
    });

    ComboBox<String> destroyCacheComboBox = new ComboBox();
    destroyCacheComboBox.setDataProvider(listDataProvider);

    destroyCacheComboBox.setPlaceholder("Select your cache");
    cacheControls.addComponent(destroyCacheComboBox);
    Button destroyCacheButton = new Button("Destroy cache");
    cacheControls.addComponent(destroyCacheButton);
    destroyCacheButton.addClickListener(event -> {
      try {
        cacheManagerBusiness.destroyCache(destroyCacheComboBox.getSelectedItem().get());
        cacheNames.remove(destroyCacheComboBox.getSelectedItem().get());
        refreshStuff(listDataProvider);
        destroyCacheComboBox.clear();
        Notification notification = new Notification("Cache destroyed with success !",
            Notification.Type.TRAY_NOTIFICATION);
        notification.setStyleName("warning");
        notification.show(Page.getCurrent());
      } catch (Exception e) {
        displayErrorNotification("Cache could not be destroyed !", ExceptionUtils.getRootCauseMessage(e));
        refreshStuff(listDataProvider);
      }
    });


    mainLayout.addComponent(cacheControls);

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
    Label header = new Label("Welcome to the tiny pounder");
    header.addStyleName(ValoTheme.LABEL_H1);
    mainLayout.addComponent(header);

  }

  private void addCacheManagerControls() {

    cacheManagerControls = new VerticalLayout();
    HorizontalLayout mainCacheManagerControls = new HorizontalLayout();


    TextArea cacheManagerConfigTextArea = new TextArea();
    cacheManagerConfigTextArea.setValue(cacheManagerBusiness.isCacheManagerAlive() ? cacheManagerBusiness.retrieveHumanReadableConfiguration() : "CacheManager configuration will be displayed here");
    cacheManagerConfigTextArea.setWidth(300, Unit.PERCENTAGE);
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
    kitPath.addValueChangeListener(event -> {
      changePathButton.setEnabled(true);
    });
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