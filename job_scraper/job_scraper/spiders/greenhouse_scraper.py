import scrapy
import json
from urllib.parse import urljoin, urlparse, parse_qs
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
        """Main parse method: get total pages, then scrape all pages"""
        self.logger.debug(f"Starting to parse Greenhouse jobs board: {response.url}")
        
        # Step 1: Get total number of pages
        total_pages = self.get_total_pages(response)
        self.logger.info(f"Found {total_pages} total pages to scrape")
        
        # Step 2: Generate requests for all pages (including current page)
        for page_num in range(1, total_pages + 1):
            if page_num == 1:
                # Process the current page immediately
                yield from self.process_page(response, page_num, total_pages)
            else:
                # Generate requests for other pages
                page_url = self.build_page_url(response.url, page_num)
                self.logger.debug(f"Requesting page {page_num}: {page_url}")
                
                yield scrapy.Request(
                    url=page_url,
                    callback=self.process_page,
                    meta={'page_num': page_num, 'total_pages': total_pages}
                )
    
    def process_page(self, response, page_num=None, total_pages=None):
        """Process a single page: extract job links and create job detail requests"""
        # Get page info from meta if not provided directly
        if page_num is None:
            page_num = response.meta['page_num']
            total_pages = response.meta['total_pages']
        
        self.logger.info(f"Processing page {page_num} of {total_pages}")
        
        # Extract job links from this page
        job_links = self.extract_job_links(response)
        self.logger.info(f"Found {len(job_links)} job links on page {page_num}")
        
        # Create requests for each job detail page
        for job_url in job_links:
            yield scrapy.Request(
                url=job_url,
                callback=self.parse_job_details
            )
    
    def get_total_pages(self, response):
        """Get the total number of pages from pagination"""
        # Check if pagination exists
        pagination_wrapper = response.css('.pagination-wrapper')
        if not pagination_wrapper:
            self.logger.info("No pagination found - single page job board")
            return 1
        
        # Get all page numbers from pagination buttons
        page_buttons = response.css('.pagination__link::text').getall()
        page_numbers = []
        
        for page_text in page_buttons:
            try:
                page_numbers.append(int(page_text.strip()))
            except ValueError:
                continue
        
        if page_numbers:
            total_pages = max(page_numbers)
            self.logger.info(f"Pagination shows page numbers: {sorted(page_numbers)}")
            return total_pages
        else:
            self.logger.info("No page numbers found in pagination - assuming single page")
            return 1
    
    def build_page_url(self, base_url, page_num):
        """Build URL for a specific page number"""
        if '?' in base_url:
            return f"{base_url}&page={page_num}"
        else:
            return f"{base_url}?page={page_num}"
    
    def extract_job_links(self, response):
        """Extract all job links from a page"""
        job_links = response.css('.job-post a::attr(href)').getall()
        
        # Convert to absolute URLs
        absolute_job_links = []
        for link in job_links:
            absolute_url = urljoin(response.url, link)
            absolute_job_links.append(absolute_url)
            self.logger.debug(f"Found job URL: {absolute_url}")
        
        return absolute_job_links
    
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

# Example with pagination:
# poetry run scrapy crawl greenhouse_jobs -a company=TheNewYorkTimes -o jobs_test.json --logfile debug.log