package com.lkg

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

class UrlDataExtractForBayesPredict(datafr: DataFrame) {

	def __INIT_Feature__( ): Seq[Array[Seq[String]]] = {
		/*
		var FILE_GOOD= spark.read.parquet(good_file_src).createOrReplaceTempView("good_url")  //sc.textFile(good_file_src)
		var FIlE__Malicious=  spark.read.load(bad_file_src).createOrReplaceTempView("Malicious_url") //sc.textFile(bad_file_src)
		val sgood=spark.sql("select url from good_url")
		val sMal=spark.sql("select url from Malicious_url  "
		println("sgood --"+sgood.count())
		println("smalicious---"+sMal.count())
		*/
		//var sdata_file=datafr.collect().map(f=>new FeatureUrl(f(0).toString()).UrlExtract_forPredict()  )   Sfeature_Bayes_Url

		val sdata_file = datafr.collect().map(f => new UrlFeatureSyntax(f(0).toString).UrlExtract_forPredict())
		Seq (sdata_file)
	}
}

class UrlFeatureSyntax(urlinfo:String) {

	var token=urlinfo

	def UrlDeal(label:Int): Seq[Any] = {
		var urlsource=token

		token=token.replace("http://","")
		token=token.replace("https://","")

		token = token.replace('.', '/')
		token = token.replace('=', '/')
		token = token.replace('&', '/')
		token = token.replace('?', '/')
		token = token.replace('-', '/')
		token = token.replace('@', '/')
		token = token.replace(':', '/')
		token = token.replace('(', '/')
		var reg="\\d+".r
		token=reg replaceAllIn (token, "")
		token.replaceAll("//", "/")
		token.replaceAll("///", "/")

		Seq(urlsource,token,label)
	}
	def UrlExtract_forPredict(): Seq[String] = {
		var urlsource=token

		token=token.replace("http://","")
		token=token.replace("https://","")

		token = token.replace('.', '/')
		token = token.replace('=', '/')
		token = token.replace('&', '/')
		token = token.replace('?', '/')
		token = token.replace('-', '/')
		token = token.replace('@', '/')
		token = token.replace(':', '/')
		token = token.replace('(', '/')
		var reg="\\d+".r
		token=reg replaceAllIn (token, "")
		token.replaceAll("//", "/")
		token.replaceAll("///", "/")

		Seq(urlsource,token)
	}
}

case class UrlFeature_Bayes_predict(Url: String, Word:String )

class LoadUrlFeatureBayesPredict(dataArr: Seq[Array[Seq[String]]], sc: SparkContext, spark: SparkSession) extends java.io.Serializable {
	def vLoad_data() = {
		var Lsc = sc.parallelize(dataArr(0).toSeq)
		var rdddf = Lsc.filter(f => f != null && f.length != 0).map(f => UrlFeature_Bayes_predict(
			f(0).asInstanceOf[String],
			f(1).toString()))
		val FeatureDF = spark.createDataFrame(rdddf)
		FeatureDF
	}
}

case class UrlFeature_Bayes_train(Url: String, Word:String, label: Int)

class LoadUrlFeatureBayesTrain(dataArrWhite_bayes: Array[Seq[Any]], dataArrBlack_bayes: Array[Seq[Any]], sc: SparkContext, spark: SparkSession) extends java.io.Serializable {
	def vLoad_data() = {
		var Lsc = sc.parallelize(dataArrWhite_bayes.toSeq ++ dataArrBlack_bayes.toSeq)

		var rdddf = Lsc.filter(f => f != null && f.nonEmpty).map(f => UrlFeature_Bayes_train(f(0).toString(),f(1).toString(),f(2).toString().toInt  ) )

		val FeatureDF = spark.createDataFrame(rdddf)
		FeatureDF.show()
		FeatureDF
	}
}
