package com.datastax.spark

import com.tuplejump.calliope.utils.RichByteBuffer
import org.apache.spark.SparkContext

import com.tuplejump.calliope.Implicits._
import com.tuplejump.calliope.CasBuilder
import com.tuplejump.calliope.Types.{CQLRowMap, CQLRowKeyMap, ThriftRowMap, ThriftRowKey}

object RiskSensitivityFilter {

  import Cql3CRDDTransformersRS._
  
  def filterByTrader(sc:SparkContext){
	      
      val cas = CasBuilder.cql3.withColumnFamily("storm_demo_cql3", "storm_risk_sensitivities_hierarchy")
      val cqlRdd = sc.cql3Cassandra[RiskSensitivity](cas)
      val riskSensitivities = cqlRdd.collect().toList

      println("FX Delta Sensitivities for trader trader0");
      riskSensitivities.filter((riskSensitivity) => riskSensitivity.subHierPath.equals("trader0"))
      	.filter((riskSensitivity) => riskSensitivity.riskSensName.equals("fxDelta"))
      	.map((riskSensitivity) => println(riskSensitivity))        
  }
  
  def filterByHier(sc:SparkContext){
        
      val cas = CasBuilder.cql3.withColumnFamily("storm_demo_cql3", "storm_risk_sensitivities_hierarchy")
      val cqlRdd = sc.cql3Cassandra[RiskSensitivity](cas)
      val riskSensitivities = cqlRdd.collect().toList
      
      println("FX Delta Sensitivities for Tokyo/FX over 1");
      riskSensitivities.filter((riskSensitivity) => riskSensitivity.hierPath.equals("Tokyo/FX"))
      	.filter((riskSensitivity) => riskSensitivity.riskSensName.equals("fxDelta"))
      	.filter((riskSensitivity) => riskSensitivity.value > 1)
      	.map((riskSensitivity) => println(riskSensitivity))
  }
  
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[1]", "castest")
    
    filterByTrader (sc);
    filterByHier(sc);
    
    sc.stop()
  }
}

case class RiskSensitivity (hierPath: String, subHierPath: String, riskSensName: String, value: Double)

private object Cql3CRDDTransformersRS {

  import RichByteBuffer._

  implicit def row2String(key: ThriftRowKey, row: ThriftRowMap): List[String] = {
    row.keys.toList
  }

  implicit def cql3Row2RiskSensitivity(keys: CQLRowKeyMap, values: CQLRowMap): RiskSensitivity = {
    RiskSensitivity(keys.get("hier_path").get, keys.get("sub_hier_path").get, keys.get("risk_sens_name").get, values.get("value").get)

  }
}