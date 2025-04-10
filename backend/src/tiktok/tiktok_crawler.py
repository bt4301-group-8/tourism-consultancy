from TikTokApi import TikTokApi
import asyncio
import os
import json

# ms_token = os.getenv("MS_TOKEN")
# manually input token for now
# ms_token = "bg4QgVI_BCybP0kx3n68mmMnH-OeJjJuC3Qf9HpsX1bqOBZPSwYWCmJXsfMZIDUpr6GcVRLuZCH6owkEwkfGox4g3g9DyWYqMHO6bNnpFjcDT1s8UXq2l7RY14O9wXJF2bvIA6YbTzAGsg=="

async def scrape_travel_hashtag(hashtag: str):
    async with TikTokApi() as api:
        # msToken refreshes every ~30 seconds or so, need to try writing a script to auto extract
        await api.create_sessions(
            headless=False,
            ms_tokens=[ms_token], 
            num_sessions=1, 
            sleep_after=3,
            browser='chromium'
        )
        
        travel_hashtag = api.hashtag(name=hashtag)
        
        # just trying to get the first video
        try:
            count = 0
            
            print(f"fetching top videos for {hashtag}")
            async for video in travel_hashtag.videos(count=1):
                with open(f"{hashtag}_top_video_metadata.json", "w") as f:
                    json.dump(video.as_dict, f, indent=2)
                
                print(f"scraped video ID: {video.id}")
                print(f"metadata saved")
                count += 1
                break  # break after the first for now
            
            if count == 0:
                print(f"no videos found for {hashtag}")
                
        except Exception as e:
            print(f"error scraping videos: {e}")

if __name__ == "__main__":
    asyncio.run(scrape_travel_hashtag())