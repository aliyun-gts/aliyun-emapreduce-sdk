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

package org.apache.spark.sql.aliyun.datahub

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.util.quoteIdentifier
import org.apache.spark.sql.catalyst.util24.escapeSingleQuotedString
import org.apache.spark.sql.sources.{BaseRelation, InsertableRelation, TableScan}
import org.apache.spark.sql.types.{StructType, StructTypeUtil}

class DatahubRelation(
    override val sqlContext: SQLContext,
    parameters: Map[String, String],
    schemaOpt: Option[StructType])
  extends BaseRelation with TableScan with InsertableRelation with Serializable with Logging {

  override def schema: StructType = DatahubSchema.getSchema(schemaOpt, parameters)

  override def buildScan(): RDD[Row] = {
    val rdd = new DatahubSourceRDD(sqlContext.sparkContext, schema, parameters)
    sqlContext.internalCreateDataFrame(rdd.setName("datahub"), schema).rdd
  }

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    val project = parameters.get(DatahubSourceProvider.OPTION_KEY_PROJECT).map(_.trim)
    val topic = parameters.get(DatahubSourceProvider.OPTION_KEY_TOPIC).map(_.trim)
    // toDDL @since spark 2.4.0, 2.3.4 don't support, by gaoju 2020-08-14
    // val schemaDDL = schema.toDDL
    val schemaDDL: String = schema.fields.map(filed => {
      val comment = filed.getComment()
        .map(escapeSingleQuotedString)
        .map(" COMMENT '" + _ + "'")

      s"${quoteIdentifier(filed.name)} ${filed.dataType.sql}${comment.getOrElse("")}"
    }).mkString(",")

    data.foreachPartition { it =>
      val encoderForDataColumns = RowEncoder(StructType.fromDDL(schemaDDL)).resolveAndBind()
      val writer = new DatahubWriter(project, topic, parameters, None)
        .createWriterFactory().createDataWriter(-1, -1, -1)
      it.foreach(t => writer.write(encoderForDataColumns.toRow(t).asInstanceOf[Row]))
    }
  }
}
