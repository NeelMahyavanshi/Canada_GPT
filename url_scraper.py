from crawl4ai import AsyncUrlSeeder, SeedingConfig
import asyncio
import json
from pathlib import Path


SCRAPED_FILE = "scraped_url.json"

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

scraped_file_path = Path(SCRAPED_FILE)
if not scraped_file_path.exists():
    scraped_file_path.write_text("[]")

async def seed_province(seeder, gov_url, prov_name):
    """Seed URLs for a single province and save results."""
    print(f"🔍 Seeding URLs for {prov_name}...")
    try:
        config = SeedingConfig(
            source="sitemap+cc", 
            max_urls=50000, 
            concurrency=100
        )

        discovered_urls = await seeder.urls(gov_url, config)
        url_list = [url['url'] for url in discovered_urls]

        # Save to individual province file
        province_file = f"{prov_name}.json"
        with open(province_file, "w") as f:
            json.dump(url_list, f, indent=4)   
            print(f"✅ Saved {len(url_list)} URLs for {prov_name} to {province_file}")


        # Append to the master list
        with open(SCRAPED_FILE, 'r+') as f:
            existing_data  = json.load(f)
            existing_data.extend(url_list)
            f.seek(0)
            json.dump(existing_data, f, indent=4)

            return url_list
        
    except Exception as e:
        print(f"❌ Failed to seed {prov_name}: {str(e)}")
        return []


async def main():
    """Main async function to process all provinces."""
    all_urls = []

    async with AsyncUrlSeeder() as seeder:
        # create task for all province
        tasks = []
        for prov_name, gov_url in canadian_provinces_websites.items():
            task = seed_province(seeder=seeder, prov_name=prov_name, gov_url=gov_url)
            tasks.append(task)

        # Run all the tasks concurrently and gather results
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # process_result
        for result in results:
            if isinstance(result, list):
                all_urls.extend(results)
            elif isinstance(result, Exception):
                print(f"Task failed with error : {result}")

        print(f"Total unique URLs discovered accross all the provinces : {len(all_urls)}")
    return all_urls


if __name__ == "__main__":
    discovered_urls = asyncio.run(main())


