package com.naresh.org
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark._
import org.apache.hadoop.hbase.util.Bytes

object HBaseTest {
  def main(args: Array[String]) {

    val hostname = System.getenv("HOSTNAME")
    val tableName = "Employee"
    val sparkConf = new SparkConf().setAppName("HBaseEmployeeDemo")
    val sc = new SparkContext(sparkConf)

    val conf = HBaseConfiguration.create()

    //Configuration setting for getting hbase data
    conf.set("zookeeper.znode.parent", "/hbase-secure")
    conf.set("hbase.client.retries.number", "10")
    conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"))
    conf.set("hadoop.security.authentication", "kerberos")
    conf.set("hbase.security.authentication", "kerberos")

    conf.set(TableInputFormat.INPUT_TABLE, tableName)
    //Check if table present
   // val hbaseAdmin = new HBaseAdmin(conf)
    //if(!hbaseAdmin.isTableAvailable(tableName))
    //{
      //val tableDesc = new HTableDescriptor(tableName)
      //tableDesc.addFamily(new HColumnDescriptor("employeeInformation".getBytes()));
      //hbaseAdmin.createTable(tableDesc)
   // }

    //Insert Data into Table

    val empTable = new HTable(conf, tableName)

    var empRecord1 = new Put(new String("1").getBytes())
    empRecord1.add("employeeInformation".getBytes(), "employeeName".getBytes(), new String("Shashi").getBytes());
    empRecord1.add("employeeInformation".getBytes(), "employeeAge".getBytes(), new String("53").getBytes());
    empRecord1.add("employeeInformation".getBytes(), "employeeDesignation".getBytes(), new String("Chief Architect").getBytes());
    empTable.put(empRecord1);

    var empRecord2 = new Put(new String("2").getBytes());
    empRecord2.add("employeeInformation".getBytes(), "employeeName".getBytes(), new String("Ankit").getBytes());
    empRecord2.add("employeeInformation".getBytes(), "employeeAge".getBytes(), new String("56").getBytes());
    empRecord2.add("employeeInformation".getBytes(), "employeeDesignation".getBytes(), new String("Data Architect").getBytes());
    empTable.put(empRecord2);

    var empRecord3 = new Put(new String("3").getBytes());
    empRecord3.add("employeeInformation".getBytes(), "employeeName".getBytes(), new String("Jitu").getBytes());
    empRecord3.add("employeeInformation".getBytes(), "employeeAge".getBytes(), new String("65").getBytes());
    empRecord3.add("employeeInformation".getBytes(), "employeeDesignation".getBytes(), new String("CEO").getBytes());
    empTable.put(empRecord3);

    var empRecord4 = new Put(new String("4").getBytes());
    empRecord4.add("employeeInformation".getBytes(), "employeeName".getBytes(), new String("Chhaaaaaaya").getBytes());
    empRecord4.add("employeeInformation".getBytes(), "employeeAge".getBytes(), new String("53").getBytes());
    empRecord4.add("employeeInformation".getBytes(), "employeeDesignation".getBytes(), new String("Chief Architect").getBytes());
    empTable.put(empRecord4);


    empTable.flushCommits()

    conf.set(TableInputFormat.INPUT_TABLE, tableName)

    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    //create RDD of result
    val resultRDD =hBaseRDD.map(x => x._2 )

    //read individual column information from RDD
    val employeeRDD = resultRDD.map(result => ((Bytes.toString(result.getValue(Bytes.toBytes("employeeInformation"), Bytes.toBytes("employeeName")))),
      (Bytes.toString(result.getValue(Bytes.toBytes("employeeInformation"), Bytes.toBytes("employeeDesignation")))),
      Bytes.toString(result.getValue(Bytes.toBytes("employeeInformation"), Bytes.toBytes("employeeAge")))))

    //Filter record from rdd
    val filteredAge = employeeRDD.filter(result => result._3.toDouble < 55)

    //Save output to Hadoop
    employeeRDD.saveAsTextFile("/user/nd2629/retail/HbaseFinalOut")

    sc.stop()
  }
}