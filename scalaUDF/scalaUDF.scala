package scalaUDF

import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.api.java.UDF2
import org.apache.spark.sql.api.java.UDF3
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

class refalt2array extends UDF1[Seq[String], List[String]]{
    override def call(ref_value : Seq[String]): List[String] = {
        val ref_value_mu = ref_value.toBuffer               
        // REF_ALT allele sort ---> AA AB BB AC BC CC AD BD CD DD ...
        val REF_temp = collection.mutable.ArrayBuffer[String]()
        REF_temp ++= Seq(ref_value_mu(0))
        for(i <-0 until ref_value_mu.size){
            for(y <- 0 until i + 1){
                REF_temp ++= Seq(ref_value_mu(y) + ref_value_mu(i)).toBuffer
            }
        }
        REF_temp.toList   
    }
}

class index2dict2 extends UDF3[Map[String, String], Seq[String], Seq[String], List[String]]{
    override def call(value : Map[String, String], format_ : Seq[String], ref_value : Seq[String]): List[String] = {
        
        val value_mu = collection.mutable.Map(value.toSeq : _*) // imutable to mutable
        val ref_value_mu = ref_value.toBuffer 
        val ref = ref_value_mu.remove(0) // GT remove and REF
        val sample_PL = value_mu("PL").split(",").toBuffer
        
        if(ref_value_mu.size == sample_PL.size){
            val sample_ALT = value_mu("GT").split(",").toBuffer
            value_mu -= ("GT", "PL")
            sample_ALT.insert(0, ref)

            // sample ALT allele sort ---> AA AB BB AC BC CC AD BD CD DD ...
            val ALT_temp = collection.mutable.ArrayBuffer[String]()
            for(i <-0 until sample_ALT.size){
                for(y <- 0 until i + 1){
                    ALT_temp ++= Seq(sample_ALT(y) + sample_ALT(i)).toBuffer
                }
            }
            
            val alt_pl = ALT_temp.zip(sample_PL).toMap
            val pl_temp = collection.mutable.ArrayBuffer[String]()

            for(i <- 0 until ref_value_mu.size){
                if(alt_pl.keySet.exists(_ == ref_value_mu(i))){
                    pl_temp += alt_pl(ref_value_mu(i))
                } else {
                    pl_temp += alt_pl("<NON_REF><NON_REF>")
                }
            }
           value_mu("PL") = pl_temp.mkString(",")
        }
        
        // format
        val format_temp = collection.mutable.ListBuffer[String]()
        for ( i <- 0 until format_.size ) {
            format_temp += "."
        }
        val format_map = format_.zip(format_temp).toMap
        val format_map_mu = collection.mutable.Map(format_map.toSeq : _*)
        val sample_key = value_mu.keys.toSeq
               
        for(i <- 0 until sample_key.size){
            if(format_map_mu.keySet.exists(_ == sample_key(i))){
                format_map_mu(sample_key(i)) = value(sample_key(i))
            }
        }
        
        // SB remove for . value
        if(format_map_mu.keySet.exists(_ == "SB")){
            if( format_map_mu("SB") == "."){
                format_map_mu -= "SB"
            }
        }
        val return_value = ListMap(format_map_mu.toSeq.sortBy(_._1):_*).values.toList // sort by keys
        return_value
    }
}
