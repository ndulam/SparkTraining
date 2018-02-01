package com.naresh.org

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions.sum



object CustomerwiseTotal
{
  def main(args: Array[String]): Unit = {

    // create Spark context with Spark configuration
    val sc = new SparkContext(new SparkConf().setAppName("CustomerwiseTotal"))
    val sqlContext = new SQLContext(sc)
    val ordersRdd = sc.textFile("/user/nd2629/retail/ordersdir/orders");
    val productsRdd = sc.textFile("/user/nd2629/retail/productsdir/products").filter(x=> x.split(",").length==6)
    val order_itemsRdd = sc.textFile("/user/nd2629/retail/order_itemsdir/order_items")
    val departmentsRdd = sc.textFile("/user/nd2629/retail/departmentsdir/departments")
    val customersRdd = sc.textFile("/user/nd2629/retail/customersdir/customers")
    val categoriesRdd = sc.textFile("/user/nd2629/retail/categoriesdir/categories")

    import sqlContext.implicits._

    val ordersdf = ordersRdd.map(line=>line.split(",")).map(tp=>orders(tp(0).toInt,tp(1),tp(2).toInt,tp(3))).toDF()
    val productsdf = productsRdd.map(line=>line.split(",")).map( tp=>products(tp(0).toInt,tp(1).toInt,tp(2),tp(3),tp(4).toInt,tp(5))).toDF
    val orderitemsdf = order_itemsRdd.map(line=>line.split(",")).map( tp=> order_items(tp(0).toInt,tp(1).toInt,tp(2).toInt,tp(3).toInt,tp(4).toFloat,tp(5).toFloat)).toDF
    val departmentsdf =  departmentsRdd.map(line=>line.split(",")).map(tp=> department(tp(0).toInt,tp(1))).toDF
    val customerdf = customersRdd.map(line=>line.split(",")).map(tp=>customers(tp(0).toInt,tp(1),tp(2),tp(3),tp(4),tp(5),tp(6),tp(7),tp(8)) ).toDF
    val categorydf = categoriesRdd.map(line=>line.split(",")).map(tp=>category(tp(0).toInt,tp(1).toInt,tp(2))).toDF
    val goi = orderitemsdf.groupBy("order_item_order_id").agg(sum("order_item_subtotal").as("sumtotal"))
    val co = customerdf.join(ordersdf, customerdf("customer_id")=== ordersdf("order_customer_id")).select("customer_fname","order_id")
    val result = goi.join(co,goi("order_item_order_id")===co("order_id")).select("customer_fname","sumtotal")

    result.coalesce(2).rdd.saveAsTextFile("/user/nd2629/retail/customerwisetotalshoping")

  }


  case class category(category_id:Int,category_department_id:Int,category_name:String)
  case class customers(customer_id:Int,customer_fname:String,customer_lname:String,customer_email:String,customer_password:String,customer_street:String,customer_city:String,customer_state:String,customer_zipcode:String)
  case class department(department_id:Int,department_name:String)
  case class order_items(order_item_id:Int,order_item_order_id:Int,order_item_product_id:Int,order_item_quantity:Int,order_item_subtotal:Float,order_item_product_price:Float)
  case class orders(order_id:Int,order_date:String,order_customer_id:Int,order_status:String)
  case class products(product_id:Int,product_category_id:Int,product_name:String,roduct_description:String,product_price:Float,product_image:String)



}