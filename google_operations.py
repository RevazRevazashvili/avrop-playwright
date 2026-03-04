from playwright.sync_api import sync_playwright
import os
import re
from urllib.parse import urljoin, urlparse
import time
import requests
from my_utilities import get_configs, setup_logging
from db_operations import DatabaseOperations
import datetime

config = get_configs("database_config")
main_logger = setup_logging()

main_logger.info(datetime.datetime.now())

sweden_domain_extensions = (
    ".se",      # Official country code Top-Level Domain (ccTLD)
    ".nu",      # Managed by the Swedish Internet Foundation; highly popular in SE
    ".com.se",  # Common second-level domain for commercial entities
    ".org.se",  # Used by non-profit organizations
    ".tm.se",   # Reserved for trademarks (often handled by specific registries)
    ".se.net",  # Alternative second-level domain
    ".se.com"   # Commercial alternative (availability may vary by registrar)
)

social_platform_names = (
    "linkedin",
    "facebook",
)
non_html_extensions = (
        ".pdf",
        ".jpg",
        ".jpeg",
        ".png",
        ".gif",
        ".svg",
        ".zip",
        ".doc",
        ".docx",
        ".xls",
        ".xlsx",
        ".ppt",
        ".pptx"
    )

def setup_db_upload_data(db_name: str, table_name: str, table_fields: list[list[str, str]], data: list[dict]) -> bool | None:
    try:
        db = DatabaseOperations()
        db.create_database(db_name)
        db.connect_to_database(db_name)
        db.create_custom_table(
            table_name,
            table_fields
        )
        db.insert_many_data(table_name, data)
        db.close()
        return True
    except Exception as e:
        print(f"error occurred in database operations: {e}")
        return False

def get_domain(url: str) -> str:
    parsed = urlparse(url)
    domain = parsed.netloc

    # handle URLs without scheme (e.g. "example.com/path")
    if not domain:
        parsed = urlparse("http://" + url)
        domain = parsed.netloc

    # remove leading "www."
    if domain.startswith("www."):
        domain = domain[4:]

    return domain

def sanitize_filename(name: str):
    return re.sub(r'[\\/*?:"<>|]', "_", name)

def get_multiple_pages(query, total_results=10):
    all_results = []
    results_per_page = 10
    pages_needed = (total_results + results_per_page - 1) // results_per_page

    api_keys = [
        'a528708e6163b0d7eb1e0b767c113641be47692e26ee5592cd9695a63adddc59',
        '0c679fddade5582913b7bd0e3d98f2f86a8888d40f7e0683be913a1ee8d114ae'
    ]

    for page in range(pages_needed):
        success = False
        for api_key in api_keys:
            params = {
                "engine": "google",
                "q": query,
                "hl": "de",
                "api_key": api_key,
                "start": page * results_per_page
            }
            try:
                response = requests.get("https://serpapi.com/search", params=params)
                data = response.json()
                if response.status_code == 200 and "organic_results" in data:
                    all_results.extend(data["organic_results"])
                    print(f"Page {page + 1}: used API key ...{api_key[-4:]}")
                    success = True
                    break
                else:
                    print(f"Page {page + 1}: API key ...{api_key[-4:]} failed")
                    if "error" in data:
                        print("Error:", data["error"])
            except Exception as e:
                print(f"Page {page + 1}: Exception {e}")
        if not success:
            break
        time.sleep(1)
        if len(all_results) >= total_results:
            break

    return [
                {
                   "url": d.get('link'),
                   "title": d.get("title"),
                   "snippet": d.get("snippet")
                } for d in all_results if 'link' in d]


def scrape_urls():
    queries = get_configs("google_queries")

    results = [
        item
        for query in queries
        for item in get_multiple_pages(query, total_results=5)
    ]

    if not results:
        print("No URLs found.")
        return

    all_information = []
    seen_urls = set()
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()

        for row in results:
            url = row.get("url")
            if not url:
                continue

            domain = get_domain(url)

            if (
                domain.endswith(sweden_domain_extensions)
                and not any(social in url.lower() for social in social_platform_names)
                and not url.lower().endswith(non_html_extensions)
                and domain not in seen_urls
            ):
                seen_urls.add(domain)
                page.goto(url, wait_until="load")

                try:
                    possible_email = page.wait_for_selector(
                        "main a[href^='mailto']"
                    ).get_attribute("href")
                except:
                    possible_email = None

                try:
                    possible_phone = page.wait_for_selector(
                        "main a[href^='tel']"
                    ).get_attribute("href")
                except:
                    possible_phone = None

                all_information.append({
                    "url": url,
                    "title": row.get("title"),
                    "snippet": row.get("snippet"),
                    "possible_email": possible_email,
                    "possible_phone": possible_phone,
                    "all_text_content": page.inner_text("body")
                })

        browser.close()

    return all_information

if __name__ == "__main__":
    results_data = scrape_urls()
    if setup_db_upload_data(
            get_configs("naming").get("database_name"),
            get_configs("naming").get("gsr_table"),
            get_configs("gsr_table_fields"),
            results_data):
        main_logger.info("all data scraped and uploaded to the database")