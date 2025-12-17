import scrapy
from datetime import datetime
import re
from ..items import JobItem


class JazzHRSpider(scrapy.Spider):
    name = "jazzHR_jobs"
    start_urls = ["https://landing.applytojob.com/apply"]

    def __init__(self, company=None, domain=None, *args, **kwargs):
        super(JazzHRSpider, self).__init__(*args, **kwargs)

        self.company = company or "landing"
        self.domain = domain or "hellolanding.com"

        self.allowed_domains = [
            f"{self.company}.applytojob.com",
            "applytojob.com",
        ]

        self.start_urls = [
            f"https://{self.company}.applytojob.com/apply"
        ]

        self.logger.info(f"JazzHR spider initialized for company: {self.company}")

    def parse(self, response):


        job_links = response.css(
            "h4.list-group-item-heading a::attr(href)"
        ).getall()

        self.logger.info(f"Found {len(job_links)} job links")

        for href in job_links:
            yield response.follow(
                href,
                callback=self.parse_job_details
            )



    def parse_job_details(self, response):

        self.logger.debug(f"Parsing job details from: {response.url}")

        title = response.css(
            ".job-header h1::text"
        ).get(default="").strip() or None

        location = response.xpath(
            "normalize-space(//div[@title='Location']/text()[last()])"
        ).get()

        department = response.xpath(
            "normalize-space(//div[@title='Department']/text()[last()])"
        ).get()

        employment_type = response.xpath(
            "normalize-space(//div[@id='resumator-job-employment']/text()[last()])"
        ).get()

        workplace_type = None

        description_container = response.xpath("//div[@id='job-description']//text()").getall()
        description = " ".join(description_container).strip()

        

        yield JobItem(
            title=title,
            employment_type=employment_type,
            workplace_type=workplace_type,
            location=location,
            department=department,
            url=response.url,
            description=description,
            requirements=None,
            company=self.company,
            source="jazzHR",
            scraped_at=datetime.now().isoformat(),
)
