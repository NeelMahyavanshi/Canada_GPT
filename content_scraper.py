# from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, RateLimiter, MemoryAdaptiveDispatcher, CacheMode, PruningContentFilter
# from crawl4ai.markdown_generation_strategy import DefaultMarkdownGenerator
# from crawl4ai.processors.pdf import PDFCrawlerStrategy, PDFContentScrapingStrategy
# import asyncio
# import json
# from pathlib import Path
# import time

# canadian_provinces_websites = {
#     "British_Columbia": "https://www2.gov.bc.ca/gov/content/home",
#     # "Alberta": "https://www.alberta.ca/",
#     # "Saskatchewan": "https://www.saskatchewan.ca/",
#     # "Manitoba": "https://manitoba.ca/",
#     # "Ontario": "https://www.ontario.ca/",
#     # "Quebec": "https://www.quebec.ca/en",
#     # "New_Brunswick": "https://www.gnb.ca/index.html",
#     # "Prince_Edward_Island": "https://www.princeedwardisland.ca/en",
#     # "Nova_Scotia": "https://novascotia.ca/",
#     # "Newfoundland_and_Labrador": "https://www.gov.nl.ca/"
# }

# def get_input_file(PROVINCE_NAME):
#     return f"{PROVINCE_NAME}.json"

# def get_output_file(PROVINCE_NAME):
#     return f"{PROVINCE_NAME}_rag_content.jsonl"

# def get_progress_file(PROVINCE_NAME):
#     return f"progress_{PROVINCE_NAME}.json"

# # Loading the Urls we discovered 
# def load_discovered_urls(PROVINCE_NAME):
#     INPUT_FILE = get_input_file(PROVINCE_NAME)
#     try:
#         with open(INPUT_FILE, "r") as f:
#             return json.load(f)
#     except FileNotFoundError:
#         print(f"File {INPUT_FILE} not found. Run the seeded for {PROVINCE_NAME} again")
#         return []
    
# def is_pdf_url(url):
#     """Check if a URL points to a PDF file"""
#     return url.lower().endswith('.pdf') or 'application/pdf' in url.lower()

# async def crawl_province(PROVINCE_NAME):

#     urls_to_crawls = load_discovered_urls(PROVINCE_NAME)[900:950]
#     if not urls_to_crawls:
#         print(f"No URLs found for {PROVINCE_NAME}. Skipping...")
#         return
    
#     # Separate PDF and HTML URLs
#     pdf_urls = [url for url in urls_to_crawls if is_pdf_url(url)]
#     html_urls = [url for url in urls_to_crawls if not is_pdf_url(url)]

#     # Crawl a batch of URLs for specific province
#     print(f"Starting to crawl {PROVINCE_NAME} with {len(urls_to_crawls)} URLs")
#     print(f" - PDFs: {len(pdf_urls)}")
#     print(f" - HTML pages: {len(html_urls)}")

#     # Configuring the crawler
#     rate_limiter = RateLimiter(
#         base_delay=(1.0,3.0),
#         max_delay=30.0,
#         max_retries=2,
#         rate_limit_codes=[429,503]
#     )

#     dispatcher = MemoryAdaptiveDispatcher(
#         max_session_permit=20,
#         rate_limiter=rate_limiter
#     )

#         # Create a pruning filter to extract only meaningful content
#     prune_filter = PruningContentFilter(
#         threshold=0.48,           
#         threshold_type="dynamic",  
#         min_word_threshold=10    
#     )

#     # Create markdown generator with the pruning filter
#     md_generator = DefaultMarkdownGenerator(content_filter=prune_filter)

#     #content extraction strategy
#     html_config = CrawlerRunConfig(
#         word_count_threshold=100,
#         exclude_external_links=False,
#         process_iframes=True,
#         verbose=True,
#         cache_mode=CacheMode.BYPASS,
#         stream=True,
#         wait_until="networkidle",
#         page_timeout=30000,
#         excluded_tags=["nav", "footer", "header", "aside", "script", "style", "form"],
#         markdown_generator=md_generator
#     )

#     # Create configurations for different content types
#     pdf_config = CrawlerRunConfig(
#         scraping_strategy=PDFContentScrapingStrategy(),  # Special PDF extraction
#         verbose=True,
#         cache_mode=CacheMode.BYPASS,
#         stream=True,
#         page_timeout=45000,  # Longer timeout for PDF processing
#     )

#     OUTPUT_FILE = get_output_file(PROVINCE_NAME)
#     Path(OUTPUT_FILE).touch()

#     processed_count = 0
#     success_count = 0

#     async with AsyncWebCrawler() as crawler:
#         if pdf_urls:
#             print(f"Processing {len(pdf_urls)} PDF documents...")
#             batch_size = 10
#             for i in range(0, len(pdf_urls), batch_size):
#                 batch = pdf_urls[i:i + batch_size]
#                 print(f"Processing PDF batch {i//batch_size + 1}/{(len(pdf_urls) - 1)//batch_size + 1}")
#                 for url in batch:
#                     async for result in await crawler.arun_many([url], config=pdf_config, dispatcher=dispatcher):
#                         processed_count += 1
                        
#                         if result.success:
#                             # PDF content is typically in result.markdown or result.raw_text
#                             pdf_content = result.markdown

#                             rag_document = {
#                                     "id": f"{PROVINCE_NAME}_pdf_{processed_count}_{int(time.time())}",
#                                     "url": result.url,
#                                     "title": result.metadata.get("title", f"PDF Document {processed_count}"),
#                                     "description": result.metadata.get("description", "Government PDF document"),
#                                     "content": pdf_content,
#                                     "province": PROVINCE_NAME,
#                                     "timestamp": time.time(),
#                                     "content_length": len(pdf_content),
#                                     "language": "en" if "/en/" in result.url else "fr",
#                                     "source": "canada_gov",
#                                     "document_type": "pdf"
#                                 }

#                             with open(OUTPUT_FILE,"a",encoding="utf-8") as f:
#                                 f.write(json.dumps(rag_document, ensure_ascii=False) + "\n")

#                             success_count += 1

#                             print(f"✅ PDF Saved ({success_count}): {rag_document['title'][:50]}... (Content length: {len(pdf_content)})") 

#                         elif not result.success:
#                             print(f"❌ Failed: {result.url} - {result.error_message}")

#                     # Saving progress report
#                     progress = {
#                         "total_processed" : processed_count,
#                         "successfull" : success_count,
#                         "last_batch_completed" : time.time(),
#                         "province" : PROVINCE_NAME
#                     }

#                     with open(f"progress_{PROVINCE_NAME}.json", "w") as f:
#                         json.dump(progress, f, indent=4)

#                     print(f"💾 Progress saved: {success_count}/{processed_count} successful")

#                     print("⏸️  Taking a 5-second break...")
#                     await asyncio.sleep(5)


#         if html_urls:
            
#             # Process URLs in manageable batches
#             batch_size = 100
#             for i in range(0, len(urls_to_crawls), batch_size):
#                 batch = urls_to_crawls[i:i + batch_size]
#                 print(f"processing batch {i//batch_size + 1}/{(len(urls_to_crawls) - 1)//batch_size+1} for {PROVINCE_NAME}")

#                 async for result in await crawler.arun_many(batch, config=html_config, dispatcher=dispatcher):
#                     processed_count += 1

#                     if result.success:

#                         # Use the fit_markdown which has been pruned of noise
#                         clean_text = result.markdown.fit_markdown

#                         rag_document = {
#                             "id" : f"{PROVINCE_NAME}_{processed_count}_{int(time.time())}",
#                             "url" : result.url,
#                             "title" : result.metadata.get("title", ""),
#                             "description" : result.metadata.get("description", ""),
#                             "content" : clean_text or "",
#                             "province" : PROVINCE_NAME,
#                             "timestamp" : time.time(),
#                             "content_length" : len(result.markdown.fit_markdown),
#                             "language" : "en" if "/en/" in result.url else "fr",
#                             "source" : "canada_gov"
#                         }

#                         with open(OUTPUT_FILE,"a",encoding="utf-8") as f:
#                             f.write(json.dumps(rag_document, ensure_ascii=False) + "\n")

#                         success_count += 1

#                         print(f"✅ Saved ({success_count}): {rag_document['title']}...")

#                     elif not result.success:
#                         print(f"❌ Failed: {result.url} - {result.error_message}")

#                 # Saving progress report
#                 progress = {
#                     "total_processed" : processed_count,
#                     "successfull" : success_count,
#                     "last_batch_completed" : time.time(),
#                     "province" : PROVINCE_NAME
#                 }

#                 with open(f"progress_{PROVINCE_NAME}.json", "w") as f:
#                     json.dump(progress, f, indent=4)

#                 print(f"💾 Progress saved: {success_count}/{processed_count} successful")

#                 print("⏸️  Taking a 5-second break...")
#                 await asyncio.sleep(5)

#         print(f"🎉 Finished! Successfully processed {success_count} documents for RAG")
#         print(f"📁 Data saved to: {OUTPUT_FILE}")

# async def crawl_all_provinces():
#     for PROVINCE_NAME, _ in canadian_provinces_websites.items():
#         print(f"\n{"="*50}")
#         print(f"STARTING CRAWL FOR : {PROVINCE_NAME}")
#         print(f"{"="*50}")

#         await crawl_province(PROVINCE_NAME)

#         print(f"\n{'='*50}")
#         print(f"COMPLETED CRAWL FOR: {PROVINCE_NAME}")
#         print(f"{'='*50}\n")

#         # Optional: Add a longer break between provinces
#         print("🔄 Taking a 5-second break before next province...")
#         await asyncio.sleep(5)

# # Run the crawler
# if __name__ == "__main__":
#     asyncio.run(crawl_all_provinces())

from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, RateLimiter, MemoryAdaptiveDispatcher, CacheMode
from crawl4ai.markdown_generation_strategy import DefaultMarkdownGenerator
from crawl4ai.content_filter_strategy import PruningContentFilter
from crawl4ai.processors.pdf import PDFContentScrapingStrategy
import asyncio
import json
from pathlib import Path
import time
import re
import aiohttp
import aiofiles
import tempfile
import os
from PyPDF2 import PdfReader

canadian_provinces_websites = {
    "British_Columbia": "https://www2.gov.bc.ca/gov/content/home",
    "Alberta": "https://www.alberta.ca/",
    "Saskatchewan": "https://www.saskatchewan.ca/",
    "Manitoba": "https://manitoba.ca/",
    "Ontario": "https://www.ontario.ca/",
    "Quebec": "https://www.quebec.ca/en",
    "New_Brunswick": "https://www.gnb.ca/index.html",
    "Prince_Edward_Island": "https://www.princeedwardisland.ca/en",
    "Nova_Scotia": "https://novascotia.ca/",
    "Newfoundland_and_Labrador": "https://www.gov.nl.ca/"
}

def get_input_file(PROVINCE_NAME):
    return f"{PROVINCE_NAME}.json"

def get_output_file(PROVINCE_NAME):
    return f"{PROVINCE_NAME}_rag_content.jsonl"

def get_progress_file(PROVINCE_NAME):
    return f"progress_{PROVINCE_NAME}.json"

def is_pdf_url(url):
    """Check if a URL points to a PDF file"""
    return url.lower().endswith('.pdf') or 'application/pdf' in url.lower()

async def download_and_extract_pdf(session, url, temp_dir):
    """Download PDF and extract text manually to avoid file locking issues"""
    try:
        # Download PDF
        async with session.get(url) as response:
            if response.status == 200:
                # Create a unique filename
                filename = os.path.join(temp_dir, f"pdf_{int(time.time())}_{hash(url)}.pdf")
                
                # Save PDF to file
                async with aiofiles.open(filename, 'wb') as f:
                    await f.write(await response.read())
                
                # Extract text from PDF
                text = ""
                try:
                    with open(filename, 'rb') as pdf_file:
                        pdf_reader = PdfReader(pdf_file)
                        for page in pdf_reader.pages:
                            page_text = page.extract_text()
                            if page_text:
                                text += page_text + "\n"
                except Exception as e:
                    print(f"Error extracting text from PDF {url}: {e}")
                    return None
                
                # Clean up
                try:
                    os.remove(filename)
                except:
                    pass
                
                return text
            else:
                print(f"Failed to download PDF {url}: HTTP {response.status}")
                return None
    except Exception as e:
        print(f"Error downloading PDF {url}: {e}")
        return None

# Loading the Urls we discovered 
def load_discovered_urls(PROVINCE_NAME):
    INPUT_FILE = get_input_file(PROVINCE_NAME)
    try:
        with open(INPUT_FILE, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"File {INPUT_FILE} not found. Run the seeder for {PROVINCE_NAME} again")
        return []
    
async def crawl_province(PROVINCE_NAME):
    urls_to_crawls = load_discovered_urls(PROVINCE_NAME)
    if not urls_to_crawls:
        print(f"No URLs found for {PROVINCE_NAME}. Skipping...")
        return

    # Separate PDF and HTML URLs
    pdf_urls = [url for url in urls_to_crawls if is_pdf_url(url)]
    html_urls = [url for url in urls_to_crawls if not is_pdf_url(url)]
    
    print(f"Starting to crawl {PROVINCE_NAME} with {len(urls_to_crawls)} URLs")
    print(f" - PDFs: {len(pdf_urls)}")
    print(f" - HTML pages: {len(html_urls)}")

    # Configuring the crawler
    rate_limiter = RateLimiter(
        base_delay=(1.0, 3.0),
        max_delay=30.0,
        max_retries=2,
        rate_limit_codes=[429, 503]
    )

    dispatcher = MemoryAdaptiveDispatcher(
        max_session_permit=20,
        rate_limiter=rate_limiter
    )

    # HTML configuration with content filtering
    prune_filter = PruningContentFilter(
        threshold=0.48,
        threshold_type="dynamic",
        min_word_threshold=10
    )

    md_generator = DefaultMarkdownGenerator(content_filter=prune_filter)

    html_config = CrawlerRunConfig(
        exclude_external_links=False,
        verbose=True,
        cache_mode=CacheMode.BYPASS,
        stream=True,
        wait_until="networkidle",
        page_timeout=30000,
        excluded_tags=["nav", "footer", "header", "aside", "script", "style", "form"],
        markdown_generator=md_generator
    )

    OUTPUT_FILE = get_output_file(PROVINCE_NAME)
    Path(OUTPUT_FILE).touch()

    processed_count = 0
    success_count = 0

    # Create a temporary directory for PDF downloads
    with tempfile.TemporaryDirectory() as temp_dir:
        async with AsyncWebCrawler() as crawler:
            # Process PDFs first with custom downloader to avoid file locking issues
            if pdf_urls:
                print(f"Processing {len(pdf_urls)} PDF documents with custom downloader...")
                
                async with aiohttp.ClientSession() as session:
                    for i, pdf_url in enumerate(pdf_urls):
                        processed_count += 1
                        print(f"Processing PDF {i+1}/{len(pdf_urls)}: {pdf_url}")
                        
                        pdf_content = await download_and_extract_pdf(session, pdf_url, temp_dir)
                        
                        if pdf_content and len(pdf_content) > 50:  # Minimum content length
                            # Extract filename from URL for title
                            url_parts = pdf_url.split('/')
                            title = url_parts[-1] if url_parts else "PDF Document"
                            
                            rag_document = {
                                "id": f"{PROVINCE_NAME}_pdf_{processed_count}_{int(time.time())}",
                                "url": pdf_url,
                                "title": title,
                                "description": "Government PDF document",
                                "content": pdf_content,
                                "province": PROVINCE_NAME,
                                "timestamp": time.time(),
                                "content_length": len(pdf_content),
                                "language": "en" if "/en/" in pdf_url else "fr",
                                "source": "canada_gov",
                                "document_type": "pdf"
                            }

                            with open(OUTPUT_FILE, "a", encoding="utf-8") as f:
                                f.write(json.dumps(rag_document, ensure_ascii=False) + "\n")

                            success_count += 1
                            print(f"✅ PDF Saved ({success_count}): {rag_document['title']}... (Content length: {len(pdf_content)})")
                        else:
                            print(f"⚠️  PDF failed or insufficient content: {pdf_url}")
                        
                        # Update progress
                        progress = {
                            "total_processed": processed_count,
                            "successful": success_count,
                            "last_result_completed": time.time(),
                            "province": PROVINCE_NAME
                        }
                        
                        progress_file = get_progress_file(PROVINCE_NAME)
                        with open(progress_file, "w") as f:
                            json.dump(progress, f, indent=4)
                        
                        # Add a delay between PDF processing to avoid overwhelming the server
                        await asyncio.sleep(2)
            
            # Process HTML pages with the regular crawler
            if html_urls:
                print(f"Processing {len(html_urls)} HTML pages...")
                batch_size = 50
                for i in range(0, len(html_urls), batch_size):
                    batch = html_urls[i:i + batch_size]
                    print(f"Processing HTML batch {i//batch_size + 1}/{(len(html_urls) - 1)//batch_size + 1} for {PROVINCE_NAME}")

                    # FIX: Don't use await with async for
                    results_stream = await crawler.arun_many(batch, config=html_config, dispatcher=dispatcher)
                    async for result in results_stream:
                        processed_count += 1

                        if result.success:
                            clean_text = ""
                            if hasattr(result, 'markdown') and result.markdown and result.markdown.fit_markdown:
                                clean_text = result.markdown.fit_markdown
                            
                            if len(clean_text) > 50:
                                rag_document = {
                                    "id": f"{PROVINCE_NAME}_{processed_count}_{int(time.time())}",
                                    "url": result.url,
                                    "title": result.metadata.get("title", ""),
                                    "description": result.metadata.get("description", ""),
                                    "content": clean_text,
                                    "province": PROVINCE_NAME,
                                    "timestamp": time.time(),
                                    "content_length": len(clean_text),
                                    "language": "en" if "/en/" in result.url else "fr",
                                    "source": "canada_gov",
                                    "document_type": "html"
                                }

                                with open(OUTPUT_FILE, "a", encoding="utf-8") as f:
                                    f.write(json.dumps(rag_document, ensure_ascii=False) + "\n")

                                success_count += 1
                                print(f"✅ HTML Saved ({success_count}): {rag_document['title']}... (Content length: {len(clean_text)})")
                            else:
                                print(f"⚠️  HTML success but insufficient content: {result.url} (Length: {len(clean_text)})")

                        elif not result.success:
                            print(f"❌ HTML Failed: {result.url} - {result.error_message}")
                        else:
                            print(f"⚠️  HTML success but no markdown content: {result.url}")

                        # Update progress
                        progress = {
                            "total_processed": processed_count,
                            "successful": success_count,
                            "last_result_completed": time.time(),
                            "province": PROVINCE_NAME
                        }

                        progress_file = get_progress_file(PROVINCE_NAME)
                        with open(progress_file, "w") as f:
                            json.dump(progress, f, indent=4)

                    print("⏸️  Taking a 2-second break between HTML batches...")
                    await asyncio.sleep(2)

    print(f"🎉 Finished! Successfully processed {success_count} documents for RAG in {PROVINCE_NAME}")
    print(f"📁 Data saved to: {OUTPUT_FILE}")

async def crawl_all_provinces():
    for PROVINCE_NAME, _ in canadian_provinces_websites.items():
        print(f"\n{'='*50}")
        print(f"STARTING CRAWL FOR: {PROVINCE_NAME}")
        print(f"{'='*50}")

        await crawl_province(PROVINCE_NAME)

        print(f"\n{'='*50}")
        print(f"COMPLETED CRAWL FOR: {PROVINCE_NAME}")
        print(f"{'='*50}\n")

        print("🔄 Taking a 5-second break before next province...")
        await asyncio.sleep(5)

# Run the crawler
if __name__ == "__main__":
    asyncio.run(crawl_all_provinces())