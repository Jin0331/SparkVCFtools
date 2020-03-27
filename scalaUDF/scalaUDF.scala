package scalaUDF

import org.apache.spark.sql.api.java.UDF2
import scala.collection.mutable
import scala.collection.immutable.ListMap

class Index2dict extends UDF2[Map[String, String], Seq[String], List[String]] {
 override def call(value : Map[String, String], format_ : Seq[String]): List[String] = {

 val value_mu = collection.mutable.Map(value.toSeq : _*) // imutable to mutable
 val format_temp = collection.mutable.ListBuffer[String]()
 for ( i <- 0 until format_.size ) {
   format_temp += "."
 }

 val format_map = format_.zip(format_temp).toMap
 val format_map_mu = collection.mutable.Map(format_map.toSeq : _*)
 val sample_key = value.keys.toSeq

 for(i <- 0 until sample_key.size){
    if(format_map_mu.keySet.exists(_ == sample_key(i))){
      format_map_mu(sample_key(i)) = value(sample_key(i))
    }
 }

 if(format_map_mu.keySet.exists(_ == "SB")){
  if( format_map_mu("SB") == "."){
    format_map_mu -= "SB"
  }
 }

 val return_value = ListMap(format_map_mu.toSeq.sortBy(_._1):_*).values.toList // sort by keys

 return_value
 }
}

