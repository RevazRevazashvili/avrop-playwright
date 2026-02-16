from playwright.sync_api import sync_playwright
import playwright
from bs4 import BeautifulSoup
from my_utilities import get_configs, setup_logging, retry
from db_operations import DatabaseOperations

main_logger = setup_logging()

config_data = get_configs("page_config")

def setup_db_upload_data(db_name: str, table_name: str, data: list[dict]) -> bool | None:
    db = None
    try:
        db = DatabaseOperations()
        db.create_database(db_name)
        db.connect_to_database(db_name)
        db.create_table(table_name)
        db.insert_many_data(table_name, data)
        db.close()
        return True
    except Exception as e:
        print(f"error occurred in database operations: {e}")
        return False
    finally:
        db.close()

@retry(3)
def fill_cpv_field(arg_page: playwright, cpv_number: str) -> None:
    arg_page.wait_for_selector("#navigationContent_CPVTextBox").fill(cpv_number)
    arg_page.wait_for_selector("#navigationContent_searchButton").click()
    arg_page.wait_for_timeout(2000)

def urls_collector():
    urls = []
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--disable-software-rasterizer",
                "--disable-extensions",
                "--disable-background-networking",
                "--disable-default-apps",
                "--disable-sync",
                "--metrics-recording-only",
                "--mute-audio",
                "--no-first-run",
                "--safebrowsing-disable-auto-update",
                "--disable-blink-features=AutomationControlled",
                "--start-maximized"
            ]
        )
        context = browser.new_context(
            no_viewport=True,
            locale="en-US",
            extra_http_headers={
                "Accept-Language": "en-US,en;q=0.9"
            }
        )
        page = context.new_page()

        page.goto(config_data['url'])

        fill_cpv_field(page, config_data['CPV code'])
        page_nums = page.locator("tr.GridViewPager tbody td").all_inner_texts()
        max_num = max([eval(num) for num in page_nums if num.isdigit()])
        for pag in range(2, max_num+2):
            for _ in range(3):
                try:
                    page.wait_for_selector("tr.rowline", timeout=10000)
                    announcements = page.locator("tr.rowline")
                    inner_urls = [("https://www.e-avrop.com" + announcements.nth(i).locator("td:nth-child(1) a").get_attribute("href"),
                                   announcements.nth(i).locator("td:nth-child(2)").inner_text().strip(),
                                   announcements.nth(i).locator("td:nth-child(3)").inner_text().strip()
                                   )
                                  for i in range(announcements.count())]
                    if inner_urls:
                        urls.extend(inner_urls)
                        break
                except Exception as e:
                    main_logger.error(f"error occurred: {e}")
                    main_logger.warning("retrying...")
                    continue
            try:
                page.wait_for_selector(f"//a[text()='{pag}']", timeout=5000).click()
                page.wait_for_timeout(1500)
            except Exception as e:
                main_logger.error(f"error occurred while paginating: {e}")
                continue

    return urls

def normalize_key(label: str) -> str:
    return (
        label.lower()
        .replace(" ", "_")
        .replace(":", "")
        .replace("(", "")
        .replace(")", "")
        .replace("å", "a")
        .replace("ä", "a")
        .replace("ö", "o")
    )

def backup_scraper(segment: BeautifulSoup, info: tuple) -> dict:
    try:
        title = segment.select_one("#mainContent_AnnouncementHead_TitleLabel").text.strip()
    except:
        title = None
    try:
        tender_organisation = info[2]
    except:
        tender_organisation = None
    try:
        place_of_performance = None
    except:
        place_of_performance = None
    try:
        type_of_procedure = segment.select_one("#mainContent_AnnouncementHead_SpecificationLabel").text.strip()
    except:
        type_of_procedure = None
    try:
        publication_date = info[1]
    except:
        publication_date = None
    try:
        request_deadline = segment.select_one("#mainContent_AnnouncementHead_LastDay").text.strip()
    except:
        request_deadline = None
    try:
        tender_deadline = None
    except:
        tender_deadline = None
    try:
        tender_valid_until = None
    except:
        tender_valid_until = None
    try:
        question_deadline = segment.select_one("#mainContent_AnnouncementHead_QuestionLabel").text.strip()
    except:
        question_deadline = None
    try:
        description = segment.select_one("#mainContent_AnnouncementHead_DescriptionBody span").text.strip()
    except:
        description = None
    try:
        additional_cpv = segment.select_one("td[colspan='2']").text.strip()
    except:
        additional_cpv = None

    return {
        "url": info[0],
        "title": title,
        "tender_organisation": tender_organisation,
        "place_of_performance": place_of_performance,
        "type_of_procedure": type_of_procedure,
        "publication_date": publication_date,
        "request_deadline": request_deadline,
        "tender_deadline": tender_deadline,
        "tender_valid_until": tender_valid_until,
        "question_deadline": question_deadline,
        "description": description,
        "additional_cpv": additional_cpv
    }

def extract_notice_data(soup: BeautifulSoup, info: tuple) -> dict:
    data: dict[str, str] = {"url": info[0]}

    # All segments that contain label + value
    try:
        segments = soup.select(".n-segment, .notice-field")
    except Exception as e:
        main_logger.error(f"no labels found: {e}")
        segments = None

    if not segments:
        return backup_scraper(soup, info)

    for segment in segments:
        label_tag = segment.select_one("label")
        if not label_tag:
            continue

        label = label_tag.get_text(strip=True)

        # Remove label so we can cleanly extract value
        label_tag.extract()

        value = segment.get_text().strip(" ").removeprefix("\xa0")

        key = normalize_key(label)

        data[key] = value

    cpvs = soup.select(".cpv-code")
    data["additional_cpv"] = "\n".join([
        cpv.get_text(strip=True)
        for cpv in cpvs
    ])

    return data

def scrape_notice(info: tuple) -> dict:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            locale="en-US",
            extra_http_headers={
                "Accept-Language": "en-US,en;q=0.9"
            }
        )
        page = context.new_page()

        # Load main page
        page.goto(info[0])

        # Wait for iframe
        iframe_element = page.wait_for_selector("iframe[id*='mainContent']", timeout=10000)

        # Switch to iframe
        frame = iframe_element.content_frame()

        # Get full iframe HTML
        html = frame.content()

        browser.close()

    if not html:
        return {"url": info[0]}

    # Parse with BeautifulSoup
    soup = BeautifulSoup(html, "lxml")

    data_to_return = extract_notice_data(soup, info)
    return data_to_return

if __name__ == "__main__":
    urls = urls_collector()
    list_of_data = [
        scrape_notice(url) for url in urls
    ]
    if list_of_data:
        if setup_db_upload_data(
                get_configs("naming").get("database_name"),
                get_configs("naming").get("table_name"),
                list_of_data):
            main_logger.info("all data scraped and uploaded to the database")
