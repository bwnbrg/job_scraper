# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class JobItem(scrapy.Item):
    # define the fields for your item here like:

    title = scrapy.Field()
    employment_type = scrapy.Field()
    workplace_type = scrapy.Field()
    location = scrapy.Field()
    department = scrapy.Field()
    url = scrapy.Field()
    description = scrapy.Field()
    requirements = scrapy.Field()
    company = scrapy.Field()
    source = scrapy.Field()  # e.g. 'lever' or 'greenhouse'
    scraped_at = scrapy.Field()