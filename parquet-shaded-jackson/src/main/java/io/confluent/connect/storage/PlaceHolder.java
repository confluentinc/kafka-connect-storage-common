/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.storage;

/**
 * This class is added as a workaround to maven. Attempting to run both verify and install phases
 * with maven fails if there's no source code in the project (the produced jar is empty).
 * @see <a href="http://mail-archives.apache.org/mod_mbox/maven-users/201608.mbox/%3c20160805151905.500d5c45@copperhead.int.arc7.info%3e">
 *   mvn clean verify deploy causes jar plugin to execute twice </a>
 *      <br>
 *      This fact subsequently fails the attempt to re-shade the dependencies without adding new
 *      code. The workaround is to introduce this class here, with the intention to remove it
 *      during shading by configuring a filter for maven-shade-plugin.
 */
public class PlaceHolder { }
