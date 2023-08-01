package org.apache.nifi.processors;

import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathFactory;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;


public class XPathHandler {

  static String evaluateXPathValue(InputStream inputStream, String xpathQuery) throws Exception {
    DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
    Document document = builder.parse(inputStream);

    XPathFactory xPathFactory = XPathFactory.newInstance();
    XPath xPath = xPathFactory.newXPath();
    XPathExpression xPathExpression = xPath.compile(xpathQuery);

    return xPathExpression.evaluate(document);
  }

  public static List<String> evaluateXPath(InputStream inputStream, String xpathQuery) throws Exception {
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    DocumentBuilder builder = factory.newDocumentBuilder();
    Document doc = builder.parse(inputStream);
    XPathFactory xPathfactory = XPathFactory.newInstance();
    XPath xpath = xPathfactory.newXPath();
    XPathExpression expr = xpath.compile(xpathQuery);
    NodeList nodes = (NodeList) expr.evaluate(doc, XPathConstants.NODESET);

    List<String> nodeListContent = new ArrayList<>();
    for (int i = 0; i < nodes.getLength(); i++) {
      nodeListContent.add(nodes.item(i).getTextContent());
    }
    return nodeListContent;
  }
}

