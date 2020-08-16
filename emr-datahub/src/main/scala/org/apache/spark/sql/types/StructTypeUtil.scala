/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.types

import org.apache.spark.sql.catalyst.util.quoteIdentifier

/**
 *
 * Author: wanglei
 * Create At: 2020/8/15 10:24 PM
 */
object StructTypeUtil {
  def toDDL(structType: StructType): String = {
    structType.fields.map(f => {
      val comment = (if (f.metadata.contains("comment")) {
        Option(f.metadata.getString("comment"))
      } else None)
        .map(escapeSingleQuotedString)
        .map(" COMMENT '" + _ + "'")

      s"${quoteIdentifier(f.name)} ${f.dataType.sql}${comment.getOrElse("")}"
    }).mkString(",")
  }

  def escapeSingleQuotedString(str: String): String = {
    val builder = StringBuilder.newBuilder

    str.foreach {
      case '\'' => builder ++= s"\\\'"
      case ch => builder += ch
    }

    builder.toString()
  }
}
