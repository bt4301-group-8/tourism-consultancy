import json
import os


def get_media_info(media):
    """parses through media object to get relevant info"""
    return {
        "date": media.taken_at or "",
        "location": media.location or "",
        "caption": media.caption_text or "",
        "comment_count": media.comment_count or 0,
        "like_count": media.like_count or 0,
        "play_count": media.play_count or 0,
    }


def get_all_media_info(medias):
    """runs for a list of medias"""
    return [get_media_info(media) for media in medias]


def normalize_city_name(city_name: str):
    return city_name.lower().replace(" ", "_").replace("-", "_").replace(",", "")


def get_location_pk(
    cl,
    lat: float,
    lng: float,
):
    """gets the location pk for a given lat and lng"""
    loc = cl.location_search(lat, lng)[0]
    if loc.pk:
        return loc.pk
    # if pk not found yet
    loc = json.loads(cl.location_build(loc))
    return loc["facebook_places_id"]


def reorganize_tourism_data(input_json_path: str = "backend/data/city_posts.json"):
    # define mapping of cities to their respective countries
    city_to_country = {
        # indonesia
        "bali": "indonesia",
        "jakarta": "indonesia",
        # brunei
        "bandarseribegawan": "brunei",
        # thailand
        "bangkok": "thailand",
        "phuket": "thailand",
        # cambodia
        "siemreap": "cambodia",
        # philippines
        "cebu": "philippines",
        "davao": "philippines",
        "manila": "philippines",
        # vietnam
        "hanoi": "vietnam",
        "hochiminh": "vietnam",
        # malaysia
        "ipoh": "malaysia",
        "johorbahru": "malaysia",
        "kuala_lumpur": "malaysia",
        "kualalumpur": "malaysia",
        "malacca": "malaysia",
        # laos
        "luangprabang": "laos",
        "vientiane": "laos",
        # myanmar
        "mandalay": "myanmar",
        "yangon": "myanmar",
        # sg
        "singapore": "singapore",
    }

    # get a set of all countries for easier lookup
    countries = set(city_to_country.values())

    # load data from input json file
    with open(input_json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    # initialize dictionary for reorganized data
    reorganized_data = {country: [] for country in countries}

    # process each key-value pair in the original data
    for key, posts in data.items():
        key_lower = key.lower()
        if key_lower in city_to_country:  # if key is a city
            country = city_to_country[key_lower]
            reorganized_data[country].extend(posts)
        elif key_lower in countries:  # if key is a country
            reorganized_data[key_lower].extend(posts)
        else:
            # handle unknown locations
            print(f"warning: unknown location '{key}', skipping...")

    # deduplicate entries based on caption field
    for country, posts in reorganized_data.items():
        unique_posts = []
        seen_captions = set()

        for post in posts:
            caption = post.get("caption", "")
            if caption not in seen_captions:
                seen_captions.add(caption)
                unique_posts.append(post)

        # update with deduplicated posts
        reorganized_data[country] = unique_posts

    # ensure output directory exists
    output_path = "backend/data/country_posts.json"
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # save reorganized data with proper formatting
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(reorganized_data, f, indent=4, sort_keys=True, default=str)

    print(f"reorganized data saved to {output_path}")
    return reorganized_data
