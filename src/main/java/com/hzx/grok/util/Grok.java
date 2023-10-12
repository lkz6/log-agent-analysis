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
package com.hzx.grok.util;

import com.google.code.regexp.MatchResult;
import com.google.code.regexp.Matcher;
import com.google.code.regexp.Pattern;
import com.hzx.grok.dictionary.GrokDictionary;
import com.hzx.util.PropertiesException;

import java.io.IOException;
import java.util.Map;

/**
 *
 * @author Israel Ekpo <israel@aicer.org>
 *
 */
public final class Grok {

  private final Pattern compiledPattern;


  public Grok(final Pattern compiledPattern) {
     this.compiledPattern = compiledPattern;
  }


  public Map<String, String> extractNamedGroups(final CharSequence rawData) {

    Matcher matcher = compiledPattern.matcher(rawData);

    if (matcher.find()) {

      MatchResult r = matcher.toMatchResult();

      if (r != null && r.namedGroups() != null) {
        return r.namedGroups();
      }
    }

    return null;
  }

  private static final void displayResults(final Map<String, String> results) {
    if (results != null) {
      for(Map.Entry<String, String> entry : results.entrySet()) {
        System.out.println(entry.getKey() + "=" + entry.getValue());
      }
    }
  }

  public static void main(String[] args) throws IOException, PropertiesException {
   
    String str1 =args[0];
    String str2 =args[1];
    test(str1,str2);
  }
  
  public static void test(String str1,String str2){
    String rawDataLine1 = str1;
    String expression = str2;
    final GrokDictionary dictionary = new GrokDictionary();
    dictionary.addBuiltInDictionaries();
    dictionary.bind();
    Grok compiledPattern = dictionary.compileExpression(expression);
    displayResults(compiledPattern.extractNamedGroups(rawDataLine1));
  }
}
