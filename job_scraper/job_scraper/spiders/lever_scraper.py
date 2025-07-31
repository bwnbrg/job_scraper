import scrapy
import json
from urllib.parse import urljoin
from datetime import datetime
import re
from ..items import JobItem

class LeverJobsSpider(scrapy.Spider):
    name = "lever_jobs"

    def __init__(self, company=None, domain=None, *args, **kwargs):
        super(LeverJobsSpider, self).__init__(*args, **kwargs)
        
        # Set default values if not provided
        self.company = company or "immuta"
        self.domain = domain or "immuta.com"
        
        # Set dynamic domains and URLs
        self.allowed_domains = [
            self.domain,
            "jobs.lever.co",
            "api.lever.co"
        ]
        
        self.start_urls = [f"https://api.lever.co/v0/postings/{self.company}"]
        
        self.logger.info(f"Spider initialized for company: {self.company}, domain: {self.domain}")

    def parse(self, response):
        self.logger.debug("Parsing response")
        postings = response.css("a").getall()
        for posting in postings:
            self.logger.debug(f"Processing posting: {posting}")
            pattern = r'<a\s+href="([^"]+)"[^>]*>(.*?)<\/a>'
            match = re.search(pattern, posting)
            if match:
                url = match.group(1)
                link_text = match.group(2)                
                self.logger.debug(f"Yielding url: {url}")
                yield scrapy.Request(
                    url=url,
                    callback=self.parse_job_details
                )

    def parse_job_details(self, response):
        """Parse the detailed job page"""
        
        self.logger.debug("Parsing job details")
        
        # Extract job details
        title = response.css(".posting-headline > h2::text").get()
        location = response.css("div .location::text").get()
        department = response.css("div .department::text").get()
        workplaceType = response.css("div .workplaceTypes::text").get()
        employmentType = response.css("div .commitment::text").get()
                
        # Extract job description
        description = ' '.join(response.xpath("//div[@data-qa='job-description']//text()").getall()).strip()
        
        # Extract requirements
        requirements = ' '.join(response.css('ul.posting-requirements *::text').getall()).strip()
        
        # Yield the complete job information
        yield JobItem(
            title = title,
            employment_type = employmentType,
            workplace_type = workplaceType,
            location = location,
            department = department,
            url = response.url,
            description = description,
            requirements = requirements,
            company = self.company,
            source = 'lever',
            scraped_at = datetime.now().isoformat()
        )

# To run this spider:
# poetry run scrapy crawl lever_jobs -o jobs.json --logfile debug.log