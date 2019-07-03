import com.atguigu.commons.conf.ConfigurationManager
import com.atguigu.commons.constant.Constants
import com.atguigu.commons.model.{UserInfo, UserVisitAction}
import com.atguigu.commons.utils._
import java.util.{Date, UUID}
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.AccumulatorV2
import scala.collection.mutable

object SessionStatisticAgg {




    def main(args: Array[String]): Unit = {

        //获取查询的限制条件
        val jsonStr = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
        val taskParam = JSONObject.fromObject(jsonStr)

        //获取全局独一无二的主键
        val taskUUID = UUID.randomUUID().toString

        //创建sparkConf
        val sparkConf = new SparkConf().setAppName("session").setMaster("local[*]")


        //创建sparkSession
        val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
        val sc = sparkSession.sparkContext
        val actionRDD = getActionRDD(sparkSession,taskParam)

//        actionRDD.foreach(println(_))

        val sessionId2ActionRDD = actionRDD.map{
            item => (item.session_id,item)
        }

        // sessionId2GroupRDD: rdd[(sid,iterable(UserVisitAction))]
        val sessionId2GroupRDD = sessionId2ActionRDD.groupByKey()
        sessionId2ActionRDD.cache()

        //获取聚合数据里面的聚合信息
        val sessionId2FullInfoRDD = getFullInfoData(sparkSession,sessionId2GroupRDD)
//        sessionId2FullInfoRDD.foreach(println(_))

        // 设置自定义累加器，实现所有数据的统计功能,注意累加器也是懒执行的
        val sessionAggrStatAccumulator = new SessionAggrStatAccumulator

        // 注册自定义累加器
        sc.register(sessionAggrStatAccumulator, "sessionAggrStatAccumulator")
        //过滤用户数据
        val filteredSessionid2AggrInfoRDD = getFilteredData(sparkSession,taskParam,sessionAggrStatAccumulator,sessionId2FullInfoRDD)

        filteredSessionid2AggrInfoRDD.foreach(println(_))
        // 对数据进行内存缓存
//        filteredSessionid2AggrInfoRDD.persist(StorageLevel.MEMORY_ONLY)
//
//        // 业务功能一：统计各个范围的session占比，并写入MySQL
        getSessionRatio(sparkSession,taskUUID ,sessionAggrStatAccumulator.value )
//
    }

    /**
      * 计算各session范围占比，并写入MySQL
      *
      * @param value
      */
    def getSessionRatio(sparkSession: SparkSession,
                        taskUUID: String,
                        value: mutable.HashMap[String, Int]) {
        // 从Accumulator统计串中获取值
        val session_count = value.getOrElse(Constants.SESSION_COUNT,1).toDouble

        val visit_length_1s_3s = value.getOrElse(Constants.TIME_PERIOD_1s_3s, 0)
        val visit_length_4s_6s = value.getOrElse(Constants.TIME_PERIOD_4s_6s, 0)
        val visit_length_7s_9s = value.getOrElse(Constants.TIME_PERIOD_7s_9s, 0)
        val visit_length_10s_30s = value.getOrElse(Constants.TIME_PERIOD_10s_30s, 0)
        val visit_length_30s_60s = value.getOrElse(Constants.TIME_PERIOD_30s_60s, 0)
        val visit_length_1m_3m = value.getOrElse(Constants.TIME_PERIOD_1m_3m, 0)
        val visit_length_3m_10m = value.getOrElse(Constants.TIME_PERIOD_3m_10m, 0)
        val visit_length_10m_30m = value.getOrElse(Constants.TIME_PERIOD_10m_30m, 0)
        val visit_length_30m = value.getOrElse(Constants.TIME_PERIOD_30m, 0)

        val step_length_1_3 = value.getOrElse(Constants.STEP_PERIOD_1_3, 0)
        val step_length_4_6 = value.getOrElse(Constants.STEP_PERIOD_4_6, 0)
        val step_length_7_9 = value.getOrElse(Constants.STEP_PERIOD_7_9, 0)
        val step_length_10_30 = value.getOrElse(Constants.STEP_PERIOD_10_30, 0)
        val step_length_30_60 = value.getOrElse(Constants.STEP_PERIOD_30_60, 0)
        val step_length_60 = value.getOrElse(Constants.STEP_PERIOD_60, 0)

        // 计算各个访问时长和访问步长的范围
        val visit_length_1s_3s_ratio = NumberUtils.formatDouble(visit_length_1s_3s / session_count, 2)
        val visit_length_4s_6s_ratio = NumberUtils.formatDouble(visit_length_4s_6s / session_count, 2)
        val visit_length_7s_9s_ratio = NumberUtils.formatDouble(visit_length_7s_9s / session_count, 2)
        val visit_length_10s_30s_ratio = NumberUtils.formatDouble(visit_length_10s_30s / session_count, 2)
        val visit_length_30s_60s_ratio = NumberUtils.formatDouble(visit_length_30s_60s / session_count, 2)
        val visit_length_1m_3m_ratio = NumberUtils.formatDouble(visit_length_1m_3m / session_count, 2)
        val visit_length_3m_10m_ratio = NumberUtils.formatDouble(visit_length_3m_10m / session_count, 2)
        val visit_length_10m_30m_ratio = NumberUtils.formatDouble(visit_length_10m_30m / session_count, 2)
        val visit_length_30m_ratio = NumberUtils.formatDouble(visit_length_30m / session_count, 2)

        val step_length_1_3_ratio = NumberUtils.formatDouble(step_length_1_3 / session_count, 2)
        val step_length_4_6_ratio = NumberUtils.formatDouble(step_length_4_6 / session_count, 2)
        val step_length_7_9_ratio = NumberUtils.formatDouble(step_length_7_9 / session_count, 2)
        val step_length_10_30_ratio = NumberUtils.formatDouble(step_length_10_30 / session_count, 2)
        val step_length_30_60_ratio = NumberUtils.formatDouble(step_length_30_60 / session_count, 2)
        val step_length_60_ratio = NumberUtils.formatDouble(step_length_60 / session_count, 2)

        // 将统计结果封装为Domain对象
        val aggrStat = SessionAggrStat(taskUUID,
            session_count.toInt, visit_length_1s_3s_ratio, visit_length_4s_6s_ratio, visit_length_7s_9s_ratio,
            visit_length_10s_30s_ratio, visit_length_30s_60s_ratio, visit_length_1m_3m_ratio,
            visit_length_3m_10m_ratio, visit_length_10m_30m_ratio, visit_length_30m_ratio,
            step_length_1_3_ratio, step_length_4_6_ratio, step_length_7_9_ratio,
            step_length_10_30_ratio, step_length_30_60_ratio, step_length_60_ratio)

        val statRDD = sparkSession.sparkContext.makeRDD(Array(aggrStat))
        import sparkSession.implicits._

        statRDD.toDF().write
                .format("jdbc")
                .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
                .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
                .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
                .option("dbtable", "session_aggr_stat")
                .mode(SaveMode.Append)
                .save()
    }

    def getFilteredData(sparkSession: SparkSession,
                        taskParam: JSONObject,
                        sessionAggrStatAccumulator: AccumulatorV2[String, mutable.HashMap[String, Int]],
                        sessionId2FullInfoRDD: RDD[(String, String)]): RDD[(String, String)] = {

        // 获取查询任务中的配置
        val startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE)
        val endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE)
        val professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS)
        val cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES)
        val sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX)
        val keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS)
        val categoryIds = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS)

        var filterInfo = (if (startAge != null) Constants.PARAM_START_AGE + "=" + startAge + "|" else "") +
                (if (endAge != null) Constants.PARAM_END_AGE + "=" + endAge + "|" else "") +
                (if (professionals != null) Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" else "") +
                (if (cities != null) Constants.PARAM_CITIES + "=" + cities + "|" else "") +
                (if (sex != null) Constants.PARAM_SEX + "=" + sex + "|" else "") +
                (if (keywords != null) Constants.PARAM_KEYWORDS + "=" + keywords + "|" else "") +
                (if (categoryIds != null) Constants.PARAM_CATEGORY_IDS + "=" + categoryIds else "")

        if (filterInfo.endsWith("\\|")) {
            filterInfo = filterInfo.substring(0, filterInfo.length() - 1)
        }

        val parameter = filterInfo

        // 根据筛选参数进行过滤
        val sessionId2FilteredRDD = sessionId2FullInfoRDD.filter {
            case (sessionid, fullInfo) =>
            // 接着，依次按照筛选条件进行过滤
            // 按照年龄范围进行过滤（startAge、endAge）
            var success = true
            if (!ValidUtils.between(fullInfo, Constants.FIELD_AGE, parameter, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE))
                success = false


            // 按照职业范围进行过滤（professionals）
            // 互联网,IT,软件
            // 互联网
            if (!ValidUtils.in(fullInfo, Constants.FIELD_PROFESSIONAL, parameter, Constants.PARAM_PROFESSIONALS))
                success = false

            // 按照城市范围进行过滤（cities）
            // 北京,上海,广州,深圳
            // 成都
            if (!ValidUtils.in(fullInfo, Constants.FIELD_CITY, parameter, Constants.PARAM_CITIES))
                success = false

            // 按照性别进行过滤
            // 男/女
            // 男，女
            if (!ValidUtils.equal(fullInfo, Constants.FIELD_SEX, parameter, Constants.PARAM_SEX))
                success = false

            // 按照搜索词进行过滤
            // 我们的session可能搜索了 火锅,蛋糕,烧烤
            // 我们的筛选条件可能是 火锅,串串香,iphone手机
            // 那么，in这个校验方法，主要判定session搜索的词中，有任何一个，与筛选条件中
            // 任何一个搜索词相当，即通过
            if (!ValidUtils.in(fullInfo, Constants.FIELD_SEARCH_KEYWORDS, parameter, Constants.PARAM_KEYWORDS))
                success = false

            // 按照点击品类id进行过滤
            if (!ValidUtils.in(fullInfo, Constants.FIELD_CLICK_CATEGORY_IDS, parameter, Constants.PARAM_CATEGORY_IDS))
                success = false

            // 如果符合任务搜索需求
            if (success) {
                sessionAggrStatAccumulator.add(Constants.SESSION_COUNT);

                // 计算访问时长范围
                def calculateVisitLength(visitLength: Long) {
                    if (visitLength >= 1 && visitLength <= 3) {
                        sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1s_3s);
                    } else if (visitLength >= 4 && visitLength <= 6) {
                        sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_4s_6s);
                    } else if (visitLength >= 7 && visitLength <= 9) {
                        sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_7s_9s);
                    } else if (visitLength >= 10 && visitLength <= 30) {
                        sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10s_30s);
                    } else if (visitLength > 30 && visitLength <= 60) {
                        sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30s_60s);
                    } else if (visitLength > 60 && visitLength <= 180) {
                        sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1m_3m);
                    } else if (visitLength > 180 && visitLength <= 600) {
                        sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_3m_10m);
                    } else if (visitLength > 600 && visitLength <= 1800) {
                        sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10m_30m);
                    } else if (visitLength > 1800) {
                        sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30m);
                    }
                }

                // 计算访问步长范围
                def calculateStepLength(stepLength: Long) {
                    if (stepLength >= 1 && stepLength <= 3) {
                        sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_1_3);
                    } else if (stepLength >= 4 && stepLength <= 6) {
                        sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_4_6);
                    } else if (stepLength >= 7 && stepLength <= 9) {
                        sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_7_9);
                    } else if (stepLength >= 10 && stepLength <= 30) {
                        sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_10_30);
                    } else if (stepLength > 30 && stepLength <= 60) {
                        sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_30_60);
                    } else if (stepLength > 60) {
                        sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_60);
                    }
                }

                // 计算出session的访问时长和访问步长的范围，并进行相应的累加
                val visitLength = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_VISIT_LENGTH).toLong
                val stepLength = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_STEP_LENGTH).toLong
                calculateVisitLength(visitLength)
                calculateStepLength(stepLength)
            }
            success
        }
        sessionId2FilteredRDD
    }

    def getFullInfoData(sparkSession: SparkSession, sessionId2GroupRDD: RDD[(String, Iterable[UserVisitAction])]) = {

        val userId2AggrInfoRDD = sessionId2GroupRDD.map{
            case (sid,iterableAction) =>
                var startTime:Date = null
                var endTime:Date = null

                var userId = -1L

                val searchKeywords = new StringBuffer("")
                val clickCategories = new StringBuffer("")

                var stepLength = 0

                for(action <- iterableAction){
                    if (userId == -1L){
                        userId = action.user_id
                    }

                    val actionTime = DateUtils.parseTime(action.action_time)
                    if (startTime == null || startTime.after(actionTime)){
                        startTime = actionTime
                    }
                    if (endTime == null || endTime.before(actionTime)){
                        endTime = actionTime
                    }

                    val searchKeyword = action.search_keyword
                    val clickCategory = action.click_category_id

                    if (StringUtils.isNotEmpty(searchKeyword) && !searchKeywords.toString.contains(searchKeyword)){
                        searchKeywords.append(searchKeyword+",")
                    }

                    if(clickCategory!= -1L && !clickCategories.toString.contains(clickCategory)){
                        clickCategories.append(clickCategory + ",")
                    }

                    stepLength += 1
                }
                val searchKw = StringUtils.trimComma(searchKeywords.toString)
                val clickCg = StringUtils.trimComma(clickCategories.toString)

                val visitLength = (endTime.getTime - startTime.getTime) / 1000

                val aggrInfo = Constants.FIELD_SESSION_ID + "=" + sid + "|" +
                        Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKw + "|" +
                        Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCg + "|" +
                        Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|" +
                        Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|" +
                        Constants.FIELD_START_TIME + "=" + DateUtils.formatDateKey(startTime)

                (userId,aggrInfo)
        }

        val sql = "select * from user_info"
        import sparkSession.implicits._

        //sparkSession.sql(sql): DataFrame DataSet[Row]
        //sparkSession.sql(sql).as[UserInfo] DataSet[UserInfo]
        //sparkSession.sql(sql).as[UserInfo].rdd RDD[UserInfo]
        //
        val userInfoRDD = sparkSession.sql(sql).as[UserInfo].rdd.map(item=>(item.user_id,item))

        userId2AggrInfoRDD.join(userInfoRDD).map{
            case (userId,(aggInfo,userInfo))=>
                val age = userInfo.age
                val professional = userInfo.professional
                val sex = userInfo.sex
                val city = userInfo.city

                val fullInfo = aggInfo + "|" + Constants.FIELD_AGE + "=" + age + "|" +
                Constants.FIELD_PROFESSIONAL + "=" + professional + "|" +
                Constants.FIELD_SEX + "=" + sex + "|" +
                Constants.FIELD_CITY + "=" + city

                val sessionId = StringUtils.getFieldFromConcatString(aggInfo,"\\|",Constants.FIELD_SESSION_ID)

                (sessionId,fullInfo)
        }
    }

    def getActionRDD(sparkSession: SparkSession, taskParam: JSONObject) ={

        val startDate = ParamUtils.getParam(taskParam,Constants.PARAM_START_DATE)
        val endDate = ParamUtils.getParam(taskParam,Constants.PARAM_END_DATE)

        val sql = "select * from user_visit_action where date>='"+startDate+"' and date <='"+endDate+"'"

        //sparkSession.sql(sql) :DataFrame DataSet[Row]
        //sparkSession.sql(sql).as[UserVisitAction] DataSet[UserVisitAction]
        import  sparkSession.implicits._
        sparkSession.sql(sql).as[UserVisitAction].rdd
    }
}
