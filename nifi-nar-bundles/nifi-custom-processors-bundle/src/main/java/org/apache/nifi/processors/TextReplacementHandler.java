package org.apache.nifi.processors;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.stream.io.StreamUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TextReplacementHandler {

  private static String normalizeReplacementString(String replacement) {
    String replacementFinal = replacement;
    if (Pattern.compile("(\\$\\D)").matcher(replacement).find()) {
      replacementFinal = Matcher.quoteReplacement(replacement);
    }
    return replacementFinal;
  }

  public static FlowFile replaceText(
          final FlowFile flowFile,
          final String type,
          final String searchValue,
          final String replacementValue,
          final String evaluateMode,
          final Charset charset,
          final int maxBufferSize,
          final ProcessSession session) throws Exception {
    if (type.equals("RegexReplace")) {
      return regexReplaceText(flowFile, searchValue, replacementValue, evaluateMode, charset, maxBufferSize, session);
    } else {
      throw new Exception("Incorrect replace strategy");
    }
  }

  private static FlowFile regexReplaceText(
          final FlowFile flowFile,
          final String searchValue,
          final String replacementValue,
          final String evaluateMode,
          final Charset charset,
          final int maxBufferSize,
          final ProcessSession session) throws Exception {
    final int numCapturingGroups = Pattern.compile(searchValue).matcher("").groupCount();

    final Pattern searchPattern = Pattern.compile(searchValue);
    final Map<String, String> additionalAttrs = new HashMap<>(numCapturingGroups);

    if (evaluateMode.equalsIgnoreCase("EntireText")) {
      final int flowFileSize = (int) flowFile.getSize();
      final int bufferSize = Math.min(maxBufferSize, flowFileSize);
      final byte[] buffer = new byte[bufferSize];

      session.read(flowFile, new InputStreamCallback() {
        @Override
        public void process(InputStream is) throws IOException {
          StreamUtils.fillBuffer(is, buffer, false);
        }
      });

      final String contentString = new String(buffer, 0, flowFileSize, charset);
      final Matcher matcher = searchPattern.matcher(contentString);

      int matches = 0;
      final StringBuffer sb = new StringBuffer();
      while (matcher.find()) {
        matches++;

        for (int i = 0; i <= matcher.groupCount(); i++) {
          additionalAttrs.put("$" + i, matcher.group(i));
        }

        String replacementFinal = normalizeReplacementString(replacementValue);

        matcher.appendReplacement(sb, replacementFinal);
      }

      if (matches > 0) {
        matcher.appendTail(sb);

        final String updatedValue = sb.toString();
        return session.write(flowFile, new OutputStreamCallback() {
          @Override
          public void process(OutputStream os) throws IOException {
            os.write(updatedValue.getBytes(charset));
          }
        });
      } else {
        return flowFile;
      }
    } else {
      throw new Exception("unsupported evaluation mode");
    }
  }
}
