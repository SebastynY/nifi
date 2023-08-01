package org.apache.nifi.processors;

import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.ControllerServiceLookup;
import org.apache.nifi.processor.ProcessContext;

import java.util.Set;

public class ControllerServiceHandler {
  private static ProcessContext context;

  public ControllerServiceHandler(ProcessContext context) {
    ControllerServiceHandler.context = context;
  }

  public static ControllerService getServiceController(String name) {
    ControllerServiceLookup lookup = context.getControllerServiceLookup();
    Set<String> serviceIds = lookup.getControllerServiceIdentifiers(ControllerService.class);
    String foundServiceId = null;
    for (String serviceId : serviceIds) {
      if (lookup.getControllerServiceName(serviceId).equals(name)) {
        foundServiceId = serviceId;
        break;
      }
    }
    return lookup.getControllerService(foundServiceId);
  }
}