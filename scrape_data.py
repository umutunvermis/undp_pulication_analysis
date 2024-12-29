import requests
from bs4 import BeautifulSoup
from PyPDF2 import PdfReader
from pdf_handler import get_pdf_content
from pymongo import MongoClient
import os
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor

# Load environment variables from .env file
load_dotenv()

# MongoDB connection
mongo_key = os.getenv("MONGO_KEY")
if not mongo_key:
    raise ValueError("MONGO_KEY environment variable not set.")

try:    
    client = MongoClient(mongo_key)
    mydb = client["unsdg"]
    mycol = mydb["publication"]
except Exception as e:
    raise ConnectionError(f"Failed to connect to MongoDB: {e}")

base_url = "https://sdgs.un.org/"

def get_cards(page_number):
    try:
        page_url = base_url + "publications?page=" + str(page_number)
        response = requests.get(page_url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        wrapper = soup.find("div", class_="view-content row row-spacing")
        if not wrapper:
            raise ValueError("No wrapper content found.")
        cards = wrapper.find_all("div", class_="card card-custom topic-01 col-sm-6 col-lg-3")
        return cards
    except Exception as e:
        raise RuntimeError(f"Error fetching cards for page {page_number}: {e}")

def get_pub_urls(card):
    try:
        pub_uri = card.find("a").get("href")
        if not pub_uri:
            raise ValueError("No URL found in card.")
        pub_url = base_url + pub_uri
        return pub_url
    except Exception as e:
        raise ValueError(f"Error extracting publication URL: {e}")

def get_pdfs_and_goals(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        
        # Extract PDFs
        wrapper = soup.find("div", class_="field field--name-field-fileurl field--type-file field--label-hidden field__items")
        if not wrapper:
            raise ValueError("No PDF wrapper found.")
        pdfs = wrapper.find_all("div", class_="row")

        # Extract Goals
        wrapper = soup.find("div", class_="goals-content")
        if not wrapper:
            raise ValueError("No goals content found.")
        goal_divs = wrapper.find_all("a")
        goals = [goal_div.string for goal_div in goal_divs if goal_div.string]

        # Extract Title
        title = soup.find("h1").string
        if not title:
            raise ValueError("No title found.")

        return title, pdfs, goals
    except Exception as e:
        raise RuntimeError(f"Error fetching PDFs and goals from URL {url}: {e}")

def get_pdf_url(pdf):
    try:
        pdf_uri = pdf.find("a").get("href")
        if not pdf_uri:
            raise ValueError("No PDF URL found.")
        pdf_url = base_url + pdf_uri
        return pdf_url
    except Exception as e:
        raise ValueError(f"Error extracting PDF URL: {e}")

def process_card(card, page_number):
    try:
        pub_url = get_pub_urls(card)
        title, pdfs, goals = get_pdfs_and_goals(pub_url)
    except Exception as e:
        print(f"Error processing publication at page {page_number}: {e}")
        return

    for pdf in pdfs:
        try:
            pdf_url = get_pdf_url(pdf)
            pdf_content = get_pdf_content(pdf_url)
            doc = {
                "title": title,
                "url": pub_url,
                "goals": goals,
                "content": pdf_content
            }
            mycol.insert_one(doc)
        except Exception as e:
            print(f"Error processing PDF at URL {pub_url}: {e}")
            continue

# main function
if __name__ == "__main__":
    last_page = 10
    with ThreadPoolExecutor(max_workers=5) as executor:
        for i in range(0, last_page):
            try:
                cards = get_cards(i)
            except Exception as e:
                print(f"Failed to fetch cards for page {i}: {e}")
                continue

            futures = [executor.submit(process_card, card, i) for card in cards]
            for future in futures:
                try:
                    future.result()
                except Exception as e:
                    print(f"Error in processing task: {e}")
