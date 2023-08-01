//package processors;
//
//import org.apache.nifi.controller.ControllerService;
//import org.apache.nifi.controller.ControllerServiceLookup;
//import org.apache.nifi.logging.ComponentLog;
//import org.apache.nifi.processor.ProcessContext;
//import org.apache.nifi.processors.IFlow;
//import org.apache.nifi.util.MockFlowFile;
//import org.apache.nifi.util.TestRunner;
//import org.apache.nifi.util.TestRunners;
//import org.junit.Before;
//import org.junit.Test;
//import org.mockito.InjectMocks;
//import org.mockito.Mock;
//import org.mockito.Mockito;
//import org.mockito.MockitoAnnotations;
//
//import java.util.HashSet;
//import java.util.List;
//import java.util.Set;
//
//import static org.junit.Assert.assertEquals;
//
//public class IFlowTest {
//  @Mock
//  private ProcessContext context;
//  private TestRunner runner;
//
//  @Mock
//  private ControllerServiceLookup controllerServiceLookup;
//
//  @InjectMocks
//  private IFlow iFlow;
//
//  @Before
//  public void setUp() {
//    MockitoAnnotations.initMocks(this);
//    runner = TestRunners.newTestRunner(IFlow.class);
//    IFlow iFlowProcessor = (IFlow) runner.getProcessor();
//    iFlowProcessor.setLogger(Mockito.mock(ComponentLog.class));
//  }
//
//  @Test
//  public void testGetServiceController() {
//    // Arrange
//    String serviceName = "testService";
//    String serviceId = "testServiceId";
//    ControllerService service = Mockito.mock(ControllerService.class);
//    ControllerServiceLookup controllerServiceLookup = Mockito.mock(ControllerServiceLookup.class);
//
//    Set<String> serviceIds = new HashSet<>();
//    serviceIds.add(serviceId);
//
//    Mockito.when(context.getControllerServiceLookup()).thenReturn(controllerServiceLookup);
//    Mockito.when(controllerServiceLookup.getControllerServiceIdentifiers(ControllerService.class))
//            .thenReturn(serviceIds);
//    Mockito.when(controllerServiceLookup.getControllerServiceName(serviceId)).thenReturn(serviceName);
//    Mockito.when(controllerServiceLookup.getControllerService(serviceId)).thenReturn(service);
//
//    // Act
//    ControllerService returnedService = iFlow.getServiceController(serviceName);
//
//    // Assert
//    assertEquals(service, returnedService);
//    Mockito.verify(controllerServiceLookup).getControllerServiceIdentifiers(ControllerService.class);
//    Mockito.verify(controllerServiceLookup).getControllerServiceName(serviceId);
//    Mockito.verify(controllerServiceLookup).getControllerService(serviceId);
//  }
//
//  @Test
//  public void testProcessorSuccessPath() {
//    runner.enqueue("");
//    runner.run();
//    runner.assertAllFlowFilesTransferred(IFlow.REL_SUCCESS);
//    List<MockFlowFile> successFiles = runner.getFlowFilesForRelationship(IFlow.REL_SUCCESS);
//    assert (successFiles.size() > 0);
//    successFiles.get(0).assertAttributeExists("hello");
//    successFiles.get(0).assertAttributeEquals("hello", "world");
//  }
//
//
//  @Test
//  public void testGetResponseWithNonNullProtocolAndCode() {
//    IFlow iFlowProcessor = new IFlow();
//    String response = iFlowProcessor.getResponse("XI", "200");
//    assertEquals(response, "XI_OK");
//  }
//
//  @Test(expected = IllegalArgumentException.class)
//  public void testGetResponseWithNullProtocolAndCode() {
//    IFlow iFlowProcessor = new IFlow();
//    iFlowProcessor.getResponse(null, null);
//  }
//}
//
