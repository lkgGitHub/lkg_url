package com.lkg

import java.net.URL

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @param datafr datafr [Dataframe]: 输入dataframe，第一列为url
 * @function: 构建特征：url, port端口, domainNameLevels域名级数长度, isIP是否含有IP, urllen URL长度,
 *           longTermCount敏感词的数目, isDing是否顶级域名, is_Prefix_sensitive是否含有敏感词后缀, ishaschn是否含有中文,
 *           execfile_len（val url_part=url.split("\\/") val execfile=url_part(url_part.length-1））,
 *           Numricinlength 下载文件含有的数字长度,label
 * @originCode: dataexplore/DataExtract.scala -> dataexplorenew/FeatureUrl.scala
 */
class UrlDataExtractForPredict(datafr: DataFrame) {

	def init_feature(): Seq[Array[Seq[Any]]] = {
		val sdata_file = datafr.collect().map(f => new FeatureUrl(f(0).toString).UrlExtract_forPredict())
		Seq (sdata_file)
	}
}

/***
 * @param urlinfo 传入URL字符串进行特征解析并提取
 */
class FeatureUrl(urlinfo:String) {

	var url=urlinfo
	val splitPattern="\\W"
	val keywordList=("","","","")

	//url链接出现(品牌关键词)、(敏感词)
	val _URL_SENSITIVE_WORDS =List ("sex", "free", "document","write","movie","javascript","eval","js", "sexmm",
		"sexmovie", "Monitor", "banking", "confirm", "@", "-", "~")
	// 顶级域名
	val _TOP_DOMAINS = List("com", "vip", "top", "win", "red", "com", "net", "org", "wang", "gov", "edu",
		"mil", "co", "biz", "name", "info", "mobi", "pro", "travel", "club", "museum", "int", "aero", "post",
		"rec", "asia", "au", "ad", "ae", "af", "ag", "ai", "al", "am", "an", "ao", "aa", "ar", "as", "at", "au",
		"aw", "az", "ba", "bb", "bd", "be", "bf", "bg", "bh", "bi", "bj", "bm", "bn", "bo", "br", "bs", "bt", "bv",
		"bw", "by", "bz", "ca", "cc", "cf", "cd", "ch", "ci", "ck", "cl", "cm", "cn", "co", "cq", "cr", "cu", "cv",
		"cx", "cy", "cz", "de", "dj", "dk", "dm", "do", "dz", "ec", "ee", "eg", "eh", "er", "es", "et", "ev", "fi",
		"fj", "fk", "fm", "fo", "fr", "ga", "gd", "ge", "gf", "gg", "gh", "gi", "gl", "gm", "gn", "gp", "gr", "gs",
		"gt", "gu", "gw", "gy", "hk", "hm", "hn", "hr", "ht", "hu", "id", "ie", "il", "im", "in", "io", "iq", "ir",
		"is", "it", "jm", "jo", "jp", "je", "ke", "kg", "kh", "ki", "km", "kn", "kp", "kr", "kw", "ky", "kz", "la",
		"lb", "lc", "li", "lk", "lr", "ls", "lt", "lu", "lv", "ly", "ma", "mc", "md", "me", "mg", "mh", "mk", "ml",
		"mm", "mn", "mo", "mp", "mq", "mr", "ms", "mt", "mu", "mv", "mw", "mx", "my", "mz", "na", "nc", "ne", "nf",
		"ng", "ni", "nl", "no", "np", "nr", "nt", "nu", "nz", "om", "qa", "pa", "pe", "pf", "pg", "ph", "pk", "pl",
		"pm", "pn", "pr", "pt", "pw", "py", "re", "rs", "ro", "ru", "rw", "sa", "sb", "sc", "sd", "se", "sg", "sh",
		"si", "sj", "sk", "sl", "sm", "sn", "so", "sr", "st", "sv", "su", "sy", "sz", "sx", "tc", "td", "tf", "tg",
		"th", "tj", "tk", "tl", "tm", "tn", "to", "tr", "tt", "tv", "tw", "tz", "ua", "ug", "uk", "um", "us", "uy",
		"uz", "va", "vc", "ve", "vg", "vi", "vn", "vu", "wf", "ws", "ye", "yt", "za", "zm", "zw", "arts", "com",
		"edu", "firm", "gov", "info", "net", "nom", "org", "rec", "store", "web")

	val _Sensitive_Prefix=List("sisx","sis","exe","apk")

	val ipPattern="((?:(?:25[0-5]|2[0-4]\\d|((1\\d{2})|([1-9]?\\d)))\\.){3}(?:25[0-5]|2[0-4]\\d|((1\\d{2})|([1-9]?\\d))))".r

	def UrlExtract_forPredict() = {
		val lst = url.indexOf("://")
		if (lst == -1){
			url = "http://"+url
		}
		val lst1 = url.indexOf("http:")

		try {
			val uri = new URL(url)
			val Protocol= uri.getProtocol()                                                              /** 协议 **/
			val port=uri.getPort() match{ case -1=>80 case others=> uri.getPort() }                      /** 端口 **/
			val host=uri.getHost()                                                                       /** 域名 **/
			val domainNameLevels=host.split("\\.").length                                         /** 域名长度 **/
			val isIP = (ipPattern findFirstIn url ) match {  case None => false case other => true }  /** 是否含有IP **/
			val urllen=url.length()                                                                 /** URL 长度 **/
			var longTermCount=0                                                                   /**敏感词的数目**/
			_URL_SENSITIVE_WORDS.foreach(f_term=>
				if (url.indexOf(f_term)  != -1 ) {
					longTermCount = longTermCount+1
				}
			)
			var _top_domain=host.split("\\.")(domainNameLevels-1)                             /**是否顶级域名**/
			var isDing=false
			_TOP_DOMAINS.foreach(f=>
				if( _top_domain.equals(f) ){
					isDing = true
				}
			)
			var is_Prefix_sensitive=false                                                  /**是否含有敏感词后缀**/
			_Sensitive_Prefix.foreach(f=>
				if( url.endsWith(f)){
					is_Prefix_sensitive=true
				}
			)
			val url_part=url.split("\\/")
			val execfile=url_part(url_part.length-1)
			val chiness = "[\\u4E00-\\u9FBF]+".r
			var ishaschn =   (chiness  findFirstIn execfile  ) match{case None=>false   case others=>  true  }   /**是否含有中文**/
			//println("ischn ----"+ishaschn+"---")
			var execfile_len=execfile.length()
			val patterns = "-?([0-9]+).([0-9]+)|-?([0-9]+)".r

			var Numricinlength=0                                                  /****下载文件含有的数字长度****/
			(patterns findAllIn  execfile).foreach(f=>
				Numricinlength=Numricinlength+f.length()
			)
			Seq(url,port,domainNameLevels,isIP,urllen,longTermCount,isDing,is_Prefix_sensitive,ishaschn,execfile_len, Numricinlength )
		}
		catch {
			case ex:Exception =>{
				println("throw an except...."+url)
			}
				Seq()
		}
	}

//	def UrlExtract(label:Int) =
//	{
//		val lst=url.indexOf("://")
//		if ( lst == -1 )
//		{
//			url="http://"+url
//		}
//		//print("URL iS----"+url+"\n")
//		val lst1=url.indexOf("http:")
//		try {
//			val uri=new URL(url)
//			val Protocol= uri.getProtocol()                                                                  /** 协议 **/
//			val port=uri.getPort() match{ case -1=>80 case others=> uri.getPort() }                        /** 端口 **/
//			val host=uri.getHost()                                                                       /** 域名 **/
//			var domainNameLevels=host.split("\\.").length                                      /** 域名长度 **/
//			val isIP=  (ipPattern findFirstIn url ) match {  case None => false case other => true }  /** 是否含有IP **/
//			val urllen=url.length()                                                                 /** URL 长度 **/
//			var longTermCount=0                                                                   /**敏感词的数目**/
//			_URL_SENSITIVE_WORDS.foreach(f_term=>
//				if (url.indexOf(f_term)  != -1 )
//				{
//					longTermCount=longTermCount+1
//				}
//			)
//			var _top_domain=host.split("\\.")(domainNameLevels-1)                             /**是否顶级域名**/
//			var isDing=false
//			_TOP_DOMAINS.foreach(f=>
//				if(  _top_domain.equals(f)  )
//				{
//					isDing = true
//				}
//			)
//			var is_Prefix_sensitive=false                                                  /**是否含有敏感词后缀**/
//			_Sensitive_Prefix.foreach(f=>
//				if(  url.endsWith(f) )
//				{
//					is_Prefix_sensitive=true
//				}
//			)
//			val url_part=url.split("\\/")
//			val execfile=url_part(url_part.length-1)
//			var chiness="[\\u4E00-\\u9FBF]+".r
//			var ishaschn =   (chiness  findFirstIn execfile  ) match{case None=>false   case others=>  true  }   /**是否含有中文**/
//			//println("ischn ----"+ishaschn+"---")
//			var execfile_len=execfile.length()
//			val patterns = "-?([0-9]+).([0-9]+)|-?([0-9]+)".r
//			var Numricinlength=0                                                  /****下载文件含有的数字长度****/
//			(patterns findAllIn  execfile).foreach(f=>
//				Numricinlength=Numricinlength+f.length()
//			)
//			Seq(url,port,domainNameLevels,isIP,urllen,longTermCount,isDing,is_Prefix_sensitive,ishaschn,execfile_len, Numricinlength ,label)
//		}
//		catch {
//			case ex:Exception =>{
//				println("throw an except...."+url)
//			}
//				Seq()
//		}
//	}
}

case class UrlFeature_v_predict(Url: String, port: Int, domainNameLevels: Int, isIP: Boolean, urllen: Int,
																longTermCount: Int, isDing: Boolean, is_Prefix_sensitive: Boolean,
																ishaschn: Boolean, execfile_len: Int, Numricinlength: Int)

class LoadUrlFeaturePredict(dataArr: Seq[Array[Seq[Any]]], sc: SparkContext, spark: SparkSession)
	extends java.io.Serializable {
	/***
	 * @parameter:
	 * @function: 构建测试数据集：转换列数据类型，转存至dataframe并返回
	 * @origin: classifynew/TrainV.scala
	 */
	def vLoad_data(): DataFrame = {
		var Lsc = sc.parallelize(dataArr(0).toSeq)
		var rdddf = Lsc.filter(f => f != null && f.length != 0).map(f => UrlFeature_v_predict(
			f(0).asInstanceOf[String],
			f(1).toString().toInt, f(2).toString().toInt, f(3).toString().toBoolean,
			f(4).toString().toInt, f(5).toString().toInt, f(6).toString().toBoolean, f(7).toString().toBoolean, f(8).toString().toBoolean, f(9).toString().toInt,
			f(10).toString().toInt))
		val FeatureDF = spark.createDataFrame(rdddf)
		FeatureDF
	}
}

case class UrlFeature_v_train(Url: String, port: Int, domainNameLevels: Int, isIP: Boolean, urllen: Int,
															longTermCount: Int, isDing: Boolean, is_Prefix_sensitive: Boolean, ishaschn: Boolean,
															execfile_len: Int, Numricinlength: Int, label: Int)


class LoadUrlFeatureTrain(dataArrWhite: Array[Seq[Any]], dataArrBlack: Array[Seq[Any]], sc: SparkContext,
													spark: SparkSession) extends java.io.Serializable {
	def vLoad_data() = {
		var Lsc = sc.parallelize(dataArrWhite.toSeq ++ dataArrBlack.toSeq)

		var rdddf = Lsc.filter(f => f != null && f.length != 0).map(f => UrlFeature_v_train(
			f(0).asInstanceOf[String], f(1).toString().toInt, f(2).toString().toInt, f(3).toString().toBoolean,
			f(4).toString().toInt, f(5).toString().toInt, f(6).toString().toBoolean, f(7).toString().toBoolean,
			f(8).toString().toBoolean, f(9).toString().toInt, f(10).toString().toInt, f(11).toString().toInt))

		val FeatureDF = spark.createDataFrame(rdddf)
		FeatureDF.show()
		FeatureDF
	}
}
