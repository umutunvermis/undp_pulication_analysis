import requests
from bs4 import BeautifulSoup
from PyPDF2 import PdfReader
from pdf_handler import get_pdf_content
from pymongo import MongoClient
import os

mongo_key = os.getenv("MONGO_KEY")  
client = MongoClient(mongo_key)
mydb = client["unsdg"]
mycol = mydb["publication"]

base_url = "https://sdgs.un.org/"

def get_cards(page_number):
    page_url = base_url + "publications?page=" + str(page_number)
    response = requests.get(page_url)
    soup = BeautifulSoup(response.text, "html.parser")
    wrapper = soup.find("div", class_="view-content row row-spacing")
    cards = wrapper.find_all("div", class_="card card-custom topic-01 col-sm-6 col-lg-3")
    return cards

def get_pub_urls(card):
    pub_uri = card.find("a").get("href")
    pub_url = base_url + pub_uri
    return pub_url

def get_pdfs_and_goals(url):
    page_url = url
    response = requests.get(page_url)
    soup = BeautifulSoup(response.text, "html.parser")
    wrapper = soup.find("div", class_="field field--name-field-fileurl field--type-file field--label-hidden field__items")
    pdfs = wrapper.find_all("div", class_="row")

    wrapper = soup.find("div", class_="goals-content")
    goal_divs = wrapper.find_all("a")
    goals = [goal_div.string for goal_div in goal_divs]

    title = soup.find("h1").string
    return title, pdfs, goals


def get_pdf_url(pdf):
    pdf_uri = pdf.find("a").get("href")
    pdf_url = base_url + pdf_uri
    return pdf_url

# main function
if __name__ == "__main__":
    last_page =  10
    for i in range(0, last_page):
        cards = get_cards(i)
        for card in cards:
            try:
                pub_urls = get_pub_urls(card)
                title, pdfs, goals = get_pdfs_and_goals(pub_urls)
            except:
                print("Error at page" + str(i))
                continue
            try:
                for pdf in pdfs:
                    pdf_url = get_pdf_url(pdf)
                    pdf_content = get_pdf_content(pdf_url)
                    doc = {
                        "title": title,
                        "url": pub_urls,
                        "goals": goals,
                        "content": pdf_content
                    }
                    mycol.insert_one(doc)
            except:
                print("Error at url" + pub_urls)
                continue
        break