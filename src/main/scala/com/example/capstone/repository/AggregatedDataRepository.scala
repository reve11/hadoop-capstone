package com.example.capstone.repository

import com.example.capstone.model.{TopCategory, TopCategoryProduct, TopCountry}

trait AggregatedDataRepository extends Serializable {
  def saveCategory(category: TopCategory)
  def saveCategories(categories: Iterable[TopCategory])
  def saveTopProduct(product: TopCategoryProduct)
  def saveTopCategoryProducts(products: Iterable[TopCategoryProduct])
  def saveTopCountry(topCountry: TopCountry)
  def saveTopCountries(topCountries: Iterable[TopCountry])
}
