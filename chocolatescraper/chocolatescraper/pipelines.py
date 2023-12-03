from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem
import psycopg2
import logging

class PriceToUSDPipeline:

    gbpToUsdRate = 1.3

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        if adapter.get('price'):

            #converting the price to a float
            floatPrice = float(adapter['price'])

            #converting the price from gbp to usd using our hard coded exchange rate
            adapter['price'] = floatPrice * self.gbpToUsdRate

            return item
        else:
            raise DropItem(f"Missing price in {item}")


class DuplicatesPipeline:

    def __init__(self):
        self.names_seen = set()

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        if adapter['name'] in self.names_seen:
            raise DropItem(f"Duplicate item found: {item!r}")
        else:
            self.names_seen.add(adapter['name'])
            return item




class SavingToPostgresPipeline(object):
    
    def __init__(self):
        self.connection = None
        self.create_connection()
    
    def create_connection(self):
        try:
            self.connection = psycopg2.connect(
                host="localhost",
                database="scrapy_tutorials",
                user="postgres",
                password="postgres"
            )
        except psycopg2.Error as e:
            logging.error(f"Error connecting to the database: {e}")
            raise
    
    def process_item(self, item, spider):
        try:
            self.store_db(item)
        except Exception as e:
            logging.error(f"Error processing item: {e}")
        finally:
            return item
    
    def store_db(self, item):
        try:
            with self.connection, self.connection.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO chocolate_products (name, price, url) VALUES (%s, %s, %s)",
                    (item["name"], item["price"], item["url"])
                )
        except psycopg2.Error as e:
            logging.error(f"Error storing item in the database: {e}")
            self.connection.rollback()
            raise
