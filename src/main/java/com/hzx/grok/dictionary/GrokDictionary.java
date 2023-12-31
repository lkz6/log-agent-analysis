/*
 * Copyright 2014 American Institute for Computing Education and Research Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Forked from https://github.com/cloudera/cdk/blob/master/cdk-morphlines/
 * cdk-morphlines-core/src/main/java/com/cloudera/cdk/morphline/stdlib/
 * GrokDictionaries.java
 *
 * com.cloudera.cdk.morphline.stdlib.GrokDictionaries was final and could not
 * be extended
 */
package com.hzx.grok.dictionary;

import com.google.code.regexp.Pattern;
import com.google.common.io.CharStreams;
import com.google.common.io.Closeables;
import com.hzx.grok.exception.GrokCompilationException;
import com.hzx.grok.util.Grok;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.HashMap;
import java.util.Map;


public final class GrokDictionary {

  private static final Logger logger = LoggerFactory.getLogger(GrokDictionary.class);

  private final Map<String, String> regexDictionary = new HashMap<String, String>();

  private boolean ready = false;

  public GrokDictionary() {

  }

  public int getDictionarySize() {
    return this.regexDictionary.size();
  }


  public Map<String, String> getRegexDictionary() {
    return new HashMap<String, String>(this.regexDictionary);
  }


  public void bind() {
    digestExpressions();
    ready = (regexDictionary.size() > 0); // Expression have been digested and we have at least one entry
  }


  private void throwErrorIfDictionaryIsNotReady() {

    if (false == ready) {
      throw new IllegalStateException("Dictionary is empty. Please add at least one dictionary and then invoke the bind() method.");
    }
  }


  public Grok compileExpression(final String expression) {

    throwErrorIfDictionaryIsNotReady();

    final String digestedExpression = digestExpressionAux(expression);

    logger.debug("Digested [" + expression + "] into [" + digestedExpression + "] before compilation");

    return new Grok(Pattern.compile(digestedExpression));
  }

  private void digestExpressions() {

    boolean wasModified = true;

    while (wasModified) {

      wasModified = false;

      for(Map.Entry<String, String> entry: regexDictionary.entrySet()) {

        String originalExpression = entry.getValue();
        String digestedExpression = digestExpressionAux(originalExpression);
        wasModified = (originalExpression != digestedExpression);

        if (wasModified) {
          entry.setValue(digestedExpression);
          break; // stop the for loop
        }
      }
    }
  }


  public String digestExpression(String originalExpression) {

    throwErrorIfDictionaryIsNotReady();

    return digestExpressionAux(originalExpression);
  }


  private String digestExpressionAux(String originalExpression) {

    final String PATTERN_START = "%{";
    final String PATTERN_STOP = "}";
    final char PATTERN_DELIMITER = ':';

    while(true) {

      int PATTERN_START_INDEX = originalExpression.indexOf(PATTERN_START);
      int PATTERN_STOP_INDEX = originalExpression.indexOf(PATTERN_STOP, PATTERN_START_INDEX + PATTERN_START.length());

      if (PATTERN_START_INDEX < 0 || PATTERN_STOP_INDEX < 0) {
        break;
      }

      String grokPattern = originalExpression.substring(PATTERN_START_INDEX + PATTERN_START.length(), PATTERN_STOP_INDEX);

      int PATTERN_DELIMITER_INDEX = grokPattern.indexOf(PATTERN_DELIMITER);

      String regexName = grokPattern;
      String groupName = null;

      if (PATTERN_DELIMITER_INDEX >= 0) {
        regexName = grokPattern.substring(0, PATTERN_DELIMITER_INDEX);
        groupName = grokPattern.substring(PATTERN_DELIMITER_INDEX + 1, grokPattern.length());
      }

      final String dictionaryValue = regexDictionary.get(regexName);

      if (dictionaryValue == null) {
        throw new GrokCompilationException("Missing value for regex name : " + regexName);
      }

      if (dictionaryValue.contains(PATTERN_START)) {
        break;
      }

      String replacement = dictionaryValue;


      if (null != groupName) {
        replacement = "(?<" + groupName + ">" + dictionaryValue + ")";
      }

      originalExpression = new StringBuilder(originalExpression).replace(PATTERN_START_INDEX, PATTERN_STOP_INDEX + PATTERN_STOP.length(), replacement).toString();
    }

    return originalExpression;
  }


  public void addDictionary(final File file) {
    try {
      addDictionaryAux(file);
    } catch (IOException e) {
      throw new GrokCompilationException(e);
    }
  }


  public void addDictionary(final InputStream inputStream) {

    try {
      addDictionaryAux(new InputStreamReader(inputStream, "UTF-8"));
    } catch (IOException e) {
      throw new GrokCompilationException(e);
    }
  }

  public void addBuiltInDictionaries() {
    addBuiltInDictionary(BuiltInDictionary.GROK_BASE);
    addBuiltInDictionary(BuiltInDictionary.GROK_JAVA);
//    addBuiltInDictionary(BuiltInDictionary.GROK_REDIS);
    addBuiltInDictionary(BuiltInDictionary.GROK_SYSLOG);
    addBuiltInDictionary(BuiltInDictionary.GROK_MONGODB);
    addBuiltInDictionary(BuiltInDictionary.GROK_POSTGRESQL);
  }

  private void addBuiltInDictionary(BuiltInDictionary dictionaryName) {

    final String filePath = dictionaryName.getFilePath();

    InputStream istream = GrokDictionary.class.getClassLoader().getResourceAsStream(filePath);

    if (istream == null) {
      istream = GrokDictionary.class.getResourceAsStream(filePath);
    }

    if (istream == null) {
      return;
    }

    logger.info("Loading Built-In dictionary: " + filePath);

    addDictionary(istream);

  }

  private void addDictionaryAux(final File file) throws IOException {

    if (false == file.exists()) {
      throw new GrokCompilationException("The path specfied could not be found: " + file);
    }

    if (false == file.canRead()) {
      throw new GrokCompilationException("The path specified is not readable" + file);
    }

    if (file.isDirectory()) {

      File[] children = file.listFiles();

      for (File child : children) {
        addDictionaryAux(child);
      }

    } else {

      Reader reader = new InputStreamReader(new FileInputStream(file), "UTF-8");

      try {
        addDictionaryAux(reader);
      } finally {
        Closeables.closeQuietly(reader);
      }
    }

  }


  public void addDictionary(Reader reader) {

    try {
      addDictionaryAux(reader);
    } catch (IOException e) {
      throw new GrokCompilationException(e);
    } finally {
      Closeables.closeQuietly(reader);
    }
  }

  private void addDictionaryAux(Reader reader) throws IOException {

    for (String currentFileLine : CharStreams.readLines(reader)) {

      final String currentLine = currentFileLine.trim();

      if (currentLine.length() == 0 || currentLine.startsWith("#")) {
        continue;
      }

      int entryDelimiterPosition = currentLine.indexOf(" ");

      if (entryDelimiterPosition < 0) {
        throw new GrokCompilationException("Dictionary entry (name and value) must be space-delimited: " + currentLine);
      }

      if (entryDelimiterPosition == 0) {
        throw new GrokCompilationException("Dictionary entry must contain a name. " + currentLine);
      }

      final String dictionaryEntryName = currentLine.substring(0, entryDelimiterPosition);
      final String dictionaryEntryValue = currentLine.substring(entryDelimiterPosition + 1, currentLine.length()).trim();

      if (dictionaryEntryValue.length() == 0) {
        throw new GrokCompilationException("Dictionary entry must contain a value: " + currentLine);
      }

      regexDictionary.put(dictionaryEntryName, dictionaryEntryValue);
    }
  }

}
