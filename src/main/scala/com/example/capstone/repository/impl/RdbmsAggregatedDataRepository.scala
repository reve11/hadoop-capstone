package com.example.capstone.repository.impl

import java.util.Properties

import com.example.capstone.model.{TopCategory, TopCategoryProduct, TopCountry}
import com.example.capstone.repository.AggregatedDataRepository
import org.apache.spark.SparkContext

class RdbmsAggregatedDataRepository(context: SparkContext, val connectionString: String, userName: String,
                                    password: String) extends AggregatedDataRepository {
  private val sqlContext = new org.apache.spark.sql.SQLContext(context)

  private val properties: Properties = new Properties
  properties.setProperty("user", userName)
  properties.setProperty("password", password)
  private val categoriesTable = "top_categories"
  private val productsTable = "top_products"
  private val countriesTable = "top_countries"


  override def saveCategory(category: TopCategory): Unit = saveCategories(List(category))

  override def saveCategories(categories: Iterable[TopCategory]): Unit = {
    val categoriesDF = sqlContext.createDataFrame(categories.toSeq)
    categoriesDF.write.mode("append").jdbc(connectionString, categoriesTable, properties)
  }

  override def saveTopProduct(product: TopCategoryProduct): Unit = saveTopCategoryProducts(List(product))

  override def saveTopCategoryProducts(products: Iterable[TopCategoryProduct]): Unit = {
    val productsDF = sqlContext.createDataFrame(products.toSeq)
    productsDF.write.mode("append").jdbc(connectionString, productsTable, properties)
  }

  override def saveTopCountry(topCountry: TopCountry): Unit = saveTopCountries(List(topCountry))

  override def saveTopCountries(topCountries: Iterable[TopCountry]): Unit = {
    val countriesDF = sqlContext.createDataFrame(topCountries.toSeq)
    countriesDF.write.mode("append").jdbc(connectionString, countriesTable, properties)
  }
}
