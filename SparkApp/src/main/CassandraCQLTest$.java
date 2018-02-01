/**
 * This example demonstrates how to read and write to cassandra column family created using CQL3
 * using Spark.
 * Parameters : <cassandra_node> <cassandra_port>
 * Usage: ./bin/spark-submit examples.jar \
 *  --class org.apache.spark.examples.CassandraCQLTest localhost 9160
 */
object CassandraCQLTest {

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("CQLTestApp")

    val sc = new SparkContext(sparkConf)
    val cHost: String = args(0)
    val cPort: String = args(1)
    val KeySpace = "retail"
    val InputColumnFamily = "ordercf"
    val OutputColumnFamily = "salecount"

    val job = new Job()
    job.setInputFormatClass(classOf[CqlPagingInputFormat])
    val configuration = job.getConfiguration
    ConfigHelper.setInputInitialAddress(job.getConfiguration(), cHost)
    ConfigHelper.setInputRpcPort(job.getConfiguration(), cPort)
    ConfigHelper.setInputColumnFamily(job.getConfiguration(), KeySpace, InputColumnFamily)
    ConfigHelper.setInputPartitioner(job.getConfiguration(), "Murmur3Partitioner")
    CqlConfigHelper.setInputCQLPageRowSize(job.getConfiguration(), "3")

    /** CqlConfigHelper.setInputWhereClauses(job.getConfiguration(), "user_id='bob'") */

    /** An UPDATE writes one or more columns to a record in a Cassandra column family */
    val query = "UPDATE " + KeySpace + "." + OutputColumnFamily + " SET sale_count = ? "
    CqlConfigHelper.setOutputCql(job.getConfiguration(), query)

    job.setOutputFormatClass(classOf[CqlOutputFormat])
    ConfigHelper.setOutputColumnFamily(job.getConfiguration(), KeySpace, OutputColumnFamily)
    ConfigHelper.setOutputInitialAddress(job.getConfiguration(), cHost)
    ConfigHelper.setOutputRpcPort(job.getConfiguration(), cPort)
    ConfigHelper.setOutputPartitioner(job.getConfiguration(), "Murmur3Partitioner")

    val casRdd = sc.newAPIHadoopRDD(job.getConfiguration(),
      classOf[CqlPagingInputFormat],
      classOf[java.util.Map[String, ByteBuffer]],
      classOf[java.util.Map[String, ByteBuffer]])

    println("Count: " + casRdd.count)
    val productSaleRDD = casRdd.map {
      case (key, value) => {
        (ByteBufferUtil.string(value.get("prod_id")), ByteBufferUtil.toInt(value.get("quantity")))
      }
    }
    val aggregatedRDD = productSaleRDD.reduceByKey(_ + _)
    aggregatedRDD.collect().foreach {
      case (productId, saleCount) => println(productId + ":" + saleCount)
    }

    val casoutputCF = aggregatedRDD.map {
      case (productId, saleCount) => {
        val outKey = Collections.singletonMap("prod_id", ByteBufferUtil.bytes(productId))
        val outVal = Collections.singletonList(ByteBufferUtil.bytes(saleCount))
        (outKey, outVal)
      }
    }

    casoutputCF.saveAsNewAPIHadoopFile(
        KeySpace,
        classOf[java.util.Map[String, ByteBuffer]],
        classOf[java.util.List[ByteBuffer]],
        classOf[CqlOutputFormat],
        job.getConfiguration()
      )

    sc.stop()
  }
}