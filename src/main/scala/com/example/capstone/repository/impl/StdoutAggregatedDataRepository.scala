package com.example.capstone.repository.impl

import com.example.capstone.model.{TopCategory, TopCategoryProduct, TopCountry}
import com.example.capstone.repository.AggregatedDataRepository

class StdoutAggregatedDataRepository extends AggregatedDataRepository{
  override def saveCategory(category: TopCategory): Unit = saveCategories(List(category))

  override def saveCategories(categories: Iterable[TopCategory]): Unit = {
    categories.foreach(println)
  }

  override def saveTopProduct(product: TopCategoryProduct): Unit = saveTopCategoryProducts(List(product))

  override def saveTopCategoryProducts(products: Iterable[TopCategoryProduct]): Unit = {
    products.foreach(println)
  }

  override def saveTopCountry(topCountry: TopCountry): Unit = saveTopCountries(List(topCountry))

  override def saveTopCountries(topCountries: Iterable[TopCountry]): Unit = {
    topCountries.foreach(println)
  }
}
