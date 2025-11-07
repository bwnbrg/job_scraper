import scrapy
import json
from urllib.parse import urljoin, urlparse, parse_qs
from datetime import datetime
from ..items import JobItem


class AshbyJobsSpider(scrapy.Spider):
    name = "asby_jobs"

    def __init__(self, company=None, domain=None, *args, **kwargs):
        super(AshbyJobsSpider,self).__init__(*args, **kwargs)

        #Set default company if not provided
        self.company = company or ""
        self.domain = domain or ""

        #potential url patterns

        #job board url patterns

        self.logger.info(f"Ashby spider initialized for company: {self.company}")

    
