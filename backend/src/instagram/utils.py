import json


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
