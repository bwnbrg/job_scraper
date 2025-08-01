import scrapy
import json
from urllib.parse import urljoin
from datetime import datetime
import re
from ..items import JobItem
from .greenhouse_scraper import GreenhouseJobsSpider
from .lever_scraper import LeverJobsSpider

#When adding a new scraper, make sure to:
# 1. Import it at the top of this file
# 2. Add it to the allowed_domains list
# 3. Initialize it in the __init__ method
# 4. Add to source_platform list in parse_job_details
# 5. Add to detect_source_platform method
# 6. Add to parse_secondary_source method
# 7. Create a new method to parse its job details, following pattern in parse_lever_job and parse_greenhouse_job


class GetroJobsSpider(scrapy.Spider):
    name = "getro_jobs"
    
    def __init__(self, company=None, domain=None, job_board_url=None, *args, **kwargs):
        super(GetroJobsSpider, self).__init__(*args, **kwargs)
        
        # Set default company if not provided
        self.company = company or "4pt0"
        self.domain = domain or "4pt0.org"
        
        # Set dynamic domains and URLs
        self.allowed_domains = [
            self.domain,
            f"jobs.{self.domain}",
            "getro.com",
            #add other domains for external platforms
            "workday.com",
            "bamboohr.com",
            "greenhouse.io",
            "lever.co"
        ]
        
        # Use provided job board URL or construct default
        if job_board_url:
            self.start_urls = [job_board_url]
            self.logger.info(f"Using provided job board URL: {job_board_url}")
        else:
            # Getro job board URL pattern
            self.start_urls = [f"https://jobs.{self.domain}/jobs"]
            self.logger.info(f"Using constructed job board URL: {self.start_urls[0]}")

        # Initialize other spiders for delegation
        self.greenhouse_spider = GreenhouseJobsSpider(company=self.company, domain=self.domain)
        self.lever_spider = LeverJobsSpider(company=self.company, domain=self.domain)
        
        self.logger.info(f"Getro spider initialized for company: {self.company}")
    
    def parse(self, response):
        """Parse the main jobs board page"""
        self.logger.debug("Parsing Getro jobs board")
        
        # Extract job links from the main page
        job_links = response.css('a[href*="/jobs/"]::attr(href)').getall()
        
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
        """Parse individual job posting details and follow apply links"""
        self.logger.debug(f"Parsing job details for: {response.url}")
        
        # Extract basic info from Getro page first
        getro_data = self.extract_getro_basic_info(response)
        
        # Extract apply URL from the apply button
        apply_url = response.css('[data-testid="button-apply-now"]::attr(href)').get()
        if not apply_url:
            apply_url = response.css('[data-testid="button"]::attr(href)').get()
        
        if apply_url:
            # Determine the source platform from the apply URL
            source_platform = self.detect_source_platform(apply_url)
            
            if source_platform in ['greenhouse', 'lever']:
                self.logger.info(f"Following {source_platform} apply link: {apply_url}")
                
                # Pass along the Getro data to the secondary parser
                yield scrapy.Request(
                    url=apply_url,
                    callback=self.parse_secondary_source,
                    meta={
                        'getro_data': getro_data,
                        'source_platform': source_platform,
                        'getro_url': response.url
                    }
                )
            else:
                # Unknown platform, use Getro data only
                self.logger.info(f"Unknown platform for apply URL: {apply_url}, using Getro data only")
                yield self.create_job_item(getro_data, job_url=response.url)
        else:
            # No apply URL found, use Getro data only
            self.logger.warning("No apply URL found, using Getro data only")
            yield self.create_job_item(getro_data, job_url=response.url)
    
    def detect_source_platform(self, url):
        """Detect the platform from the apply URL"""
        if 'greenhouse.io' in url:
            return 'greenhouse'
        elif 'lever.co' in url:
            return 'lever'
        elif 'workday.com' in url:
            return 'workday'
        elif 'bamboohr.com' in url:
            return 'bamboohr'
        else:
            return 'unknown'
    
    def extract_getro_basic_info(self, response):
        """Extract basic job info from Getro page"""
        # Extract job title - it's in an h2 with specific styling
        title = response.css('h2[font-size="28px"]::text').get()
        
        # Extract company name - look for the company logo alt text
        secondary_company = response.css('[data-testid="image"]::attr(alt)').get()
        
        # Extract job metadata from the info section
        info_texts = response.css('[data-testid="content"] *::text').getall()
        
        # Parse the info texts to extract structured data
        employment_type = None
        workplace_type = None
        location = None
        department = None
        location = None

        LOCATION_PATTERN = re.compile(r'[A-Za-z\s]+,\s*[A-Z]{2}')
        
        for text in info_texts:
            clean_text = text.strip()
            if not clean_text or clean_text == secondary_company or clean_text == title:
                continue
                
            # Check if it's a employment_type (common employment_type keywords)
            if employment_type is None:
                employment_indicators = ['full-time', 'full time', 'part-time', 'part time']
                for indicator in employment_indicators:
                    if indicator in clean_text.lower():
                        employment_type = indicator
                        break
            # Check if it's a workplace_type (common workplace_type keywords)
            if workplace_type is None:
                workplace_indicators = ['remote', 'hybrid', 'on-site']
                for indicator in workplace_indicators:
                    if indicator in clean_text.lower():
                        workplace_type = indicator
                        break    

            # Check if it's a location (using regex to match city/state pattern e.g. 'San Francisco, CA')
            if location is None and LOCATION_PATTERN.search(clean_text.strip()):
                location = LOCATION_PATTERN.search(clean_text.strip()).group(0)
            # Check if it's a department (common department keywords)
            if department is None:
                department_indicators = ['sales', 'engineering', 'marketing', 'product', 'design', 'operations', 'human resources', 'finance', 'business development']
                for indicator in department_indicators:
                    if indicator in clean_text.lower():
                        department = indicator
                        break
                
        return {
            'getro_title': title,
            'secondary_company': secondary_company,
            'getro_employment_type': employment_type,
            'getro_workplace_type': workplace_type,
            'getro_location': location,
            'getro_department': department,
            'getro_description': ' '.join(info_texts).strip(),
        }
    
    def parse_secondary_source(self, response):
        """Parse job details from secondary source (Greenhouse, Lever, etc.)"""
        getro_data = response.meta['getro_data']
        source_platform = response.meta['source_platform']
        getro_url = response.meta['getro_url']
        
        self.logger.debug(f"Parsing {source_platform} job details")
        
        if source_platform == 'greenhouse':
            yield from self.parse_greenhouse_job(response, getro_data, getro_url)
        elif source_platform == 'lever':
            yield from self.parse_lever_job(response, getro_data, getro_url)
    
    def parse_greenhouse_job(self, response, getro_data, getro_url):
        """Parse Greenhouse job details using the existing Greenhouse spider"""
        self.logger.debug("Delegating to Greenhouse spider")
        
        if '/jobs/' not in response.url:
            self.logger.warning(f"URL {response.url} doesn't appear to be a job details page, skipping")
            return

        # Call the greenhouse spider's parse_job_details method
        for job_item in self.greenhouse_spider.parse_job_details(response):
            # Only modify source and company
            secondary_company = re.search(r'.*greenhouse\.io\/(.*)\/jobs\/', response.url).group(1)
            self.logger.debug(f"extracted secondary company: {secondary_company}")
            job_item['source'] = 'getro / greenhouse'
            job_item['company'] = f"{self.company} / {secondary_company}"
            yield job_item
    
    def parse_lever_job(self, response, getro_data, getro_url):
        """Parse Lever job details using the existing Lever spider"""
        self.logger.debug("Delegating to Lever spider")

        url_match_pattern = re.compile(r'.*jobs\.lever\.co\/(.*)\/.')
        lever_job_url_confirmation = url_match_pattern.search(response.url)
        if lever_job_url_confirmation is None:
            self.logger.warning(f"URL {response.url} doesn't appear to be a job details page, skipping")
            return
        
        # Call the lever spider's parse_job_details method
        for job_item in self.lever_spider.parse_job_details(response):
            # Only modify source and company
            secondary_company = url_match_pattern.search(response.url).group(1)
            self.logger.debug(f"extracted secondary company: {secondary_company}")
            job_item['source'] = 'getro / lever'
            job_item['company'] = f"{self.company} / {secondary_company}"
            yield job_item

    def create_job_item(self, getro_data, job_url):
        """Create a JobItem using only Getro data when secondary source is unavailable"""
        
        # Create combined company name
        combined_company = f"{self.company} / {getro_data.get('secondary_company', 'Unknown')}"
        
        return JobItem(
            title=getro_data.get('getro_title'),
            employment_type=getro_data.get('getro_employment_type'),
            workplace_type=getro_data.get('getro_workplace_type'),
            location=getro_data.get('getro_location'),
            department=getro_data.get('getro_department'),
            url=job_url,
            description=getro_data.get('getro_description'),
            requirements='',
            company=combined_company,
            source='getro',
            scraped_at=datetime.now().isoformat()
        )



# To run this spider:
# poetry run scrapy crawl getro_jobs -o jobs.json --logfile debug.log
# Or with a specific company:
# poetry run scrapy crawl getro_jobs -a company=yourcompany -o jobs.json