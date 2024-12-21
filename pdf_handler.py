from pymongo import MongoClient
import PyPDF2
import requests as rq
import os
from pathlib import Path
from string import digits
from text_handler import processed_text

# Ensure the /tmp/ directory exists
if not os.path.exists("/tmp/"):
    os.makedirs("/tmp/")
    os.chmod("/tmp/", 0o777)

def is_irrelevant_page(text):
    """
    Checks if the page contains irrelevant content like TOC or References.
    Returns True if page should be skipped.
    """
    irrelevant_keywords = ["table of contents", "references", "bibliography", 
                           "index", "appendix"]
    
    text_lower = text.lower()
    for keyword in irrelevant_keywords:
        if keyword in text_lower:
            return True
    return False

def get_pdf_content(url):
    try:
        # Download PDF
        res = rq.get(url, stream=True)

        # Save PDF to temporary location
        filename = Path('/tmp/temp.pdf')
        filename.write_bytes(res.content)

        # Open PDF file
        pdfFileObj = open('/tmp/temp.pdf', 'rb')
        pdfReader = PyPDF2.PdfReader(pdfFileObj)

        pdf_content = ""

        for page_number, page in enumerate(pdfReader.pages):
            # Extract text
            s = page.extract_text()
            if s:  # Ensure there is content on the page
                # Remove digits
                remove_digits = str.maketrans('', '', digits)
                text = s.translate(remove_digits)
                
                # Check if page is irrelevant
                if is_irrelevant_page(text):
                    print(f"Skipping page {page_number + 1}: Contains irrelevant content.")
                    continue

                # Add text to the final content
                pdf_content += " " + processed_text(text)

        pdfFileObj.close()
        return pdf_content

    except Exception as e:
        print(f"Error processing URL {url}: {e}")
        return ""
