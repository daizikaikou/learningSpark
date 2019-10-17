import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Zhaogw&Lss on 2019/10/17.
  */
object MysqlRDD {
  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[*]").setAppName("Map").set("spark.testing.memory","2147480000")
    //创建spark上下文对象
    val sc = new SparkContext(config)

    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://localhost:3306/tmall_ssm"
    val userName = "root"
    val passWd = "admin"
    //创建jdbc rdd
   /* val rdd = new JdbcRDD(sc, () => {
      Class.forName(driver)
      DriverManager.getConnection(url, userName, passWd)
    },
      "select * from `user` where `id`>=? and id <=?;",
      1,
      10,
      1,
      r => (r.getInt(1), r.getString(2))
    )

    //打印最后结果
    println(rdd.count())
    rdd.foreach(println)
*/

    //插入数据
    val value: RDD[(String, String)] = sc.makeRDD(List(("zhangsan","zhangsan"),("zhangsan1","zhangsan1")))
    value.foreach{
      case (name,password)=>{
        Class.forName(driver)
        val connection: Connection = DriverManager.getConnection(url, userName, passWd)
        var sql = "insert into user (name,password) values (?,?) "
        val prepareStatement: PreparedStatement = connection.prepareStatement(sql)
        prepareStatement.setString(1,name)
        prepareStatement.setString(2,password)
        prepareStatement.executeUpdate()
        prepareStatement.close()

      }
    }


    sc.stop()
  }


}
