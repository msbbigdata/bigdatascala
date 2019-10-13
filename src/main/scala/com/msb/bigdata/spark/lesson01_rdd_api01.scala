package com.msb.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object lesson01_rdd_api01 {


  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("test01")
    val sc = new SparkContext(conf)


    val dataRDD: RDD[Int] = sc.parallelize( List(1,2,3,4,5,4,3,2,1) )

//    dataRDD.map()
//    dataRDD.flatMap()
//    dataRDD.filter((x:Int)=>{ x> 3})
    val filterRDD: RDD[Int] = dataRDD.filter(   _> 3    )
    val res01: Array[Int] = filterRDD.collect()
    res01.foreach(println)
    println("-----------------------")

    val res: RDD[Int] = dataRDD.map((_,1)).reduceByKey(_+_).map(_._1)
    res.foreach(println)

    val resx: RDD[Int] = dataRDD.distinct()
    resx.foreach(println)

    //面向数据集开发  面向数据集的API  1，基础API   2，复合API
    //RDD  （HadoopRDD,MappartitionsRDD,ShuffledRDD...）
    //map,flatMap,filter
    //distinct...
    //reduceByKey:  复合  ->  combineByKey（）


    //  面向数据集：  交并差  关联 笛卡尔积









  }

}
