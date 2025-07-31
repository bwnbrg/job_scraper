import scrapy
import json
from urllib.parse import urljoin
from datetime import datetime
from ..items import JobItem

class GreenhouseJobsSpider(scrapy.Spider):
    name = "greenhouse_jobs"
    
    def __init__(self, company=None, domain=None, *args, **kwargs):
        super(GreenhouseJobsSpider, self).__init__(*args, **kwargs)
        # Set default company if not provided
        self.company = company or "nomadhealth"
        self.domain = domain or "nomadhealth.com"
        
        # Greenhouse uses different URL patterns
        self.allowed_domains = [
            self.domain,
            "job-boards.greenhouse.io"
        ]
        
        # Greenhouse job board URL pattern
        self.start_urls = [f"https://job-boards.greenhouse.io/{self.company}"]
        
        self.logger.info(f"Greenhouse spider initialized for company: {self.company}")
    
    def parse(self, response):
        """Parse the main jobs board page"""
        self.logger.debug("Parsing Greenhouse jobs board")
        
        # Extract job links from the main page
        job_links = response.css('.job-post a::attr(href)').getall()
        
        # Log what we found
        self.logger.info(f"Found {len(job_links)} job links")
        
        for link in job_links:
            absolute_url = urljoin(response.url, link)
            self.logger.debug(f"Following job URL: {absolute_url}")
            
            yield scrapy.Request(
                url=absolute_url,
                callback=self.parse_job_details
            )
    
    def parse_job_details(self, response):
        """Parse individual job posting details"""
        self.logger.debug(f"Parsing job details for: {response.url}")
        
        # Extract job details using Greenhouse-specific selectors
        title = response.css(".job__title > h1::text").get()
        
        # Location extraction
        location = response.css('.job__location > div::text').get()
        
        # Department extraction - Greenhouse does not show departments in a consistent way
        
        # Job description
        description_parts = response.css('.job__description *::text').getall()
        
        description = ' '.join([part.strip() for part in description_parts if part.strip()])
        
        # Requirements - Greenhouse does not show departments in a consistent way, rather they are part of the job description
        
        # Yield the job data
        yield JobItem(
            title = title,
            # employment_type = employmentType,
            # workplace_type = workplaceType,
            location = location,
            # department = department,
            url = response.url,
            description = description,
            # requirements = requirements,
            company = self.company,
            source = 'greenhouse',
            scraped_at = datetime.now().isoformat()
        )

# To run this spider:
# poetry run scrapy crawl greenhouse_jobs -o jobs.json --logfile debug.log
# Or with a specific company:
# poetry run scrapy crawl greenhouse_jobs -a company=yourcompany -o jobs.json