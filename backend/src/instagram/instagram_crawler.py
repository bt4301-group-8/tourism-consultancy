import json
import os

from instagrapi import Client
from instagrapi.exceptions import LoginRequired
from typing import List, Dict, Literal, Any

from backend.src.services import logger
from backend.src.instagram.utils import (
    get_all_media_info,
    normalize_city_name,
    get_location_pk,
)


class InstagramCrawler:
    def __init__(
        self,
        delay_range: List[int] = [1, 3],
        session_json_path: str = "backend/configs/session.json",
        city_geo_json_path: str = "backend/configs/city_geo.json",
    ):
        # for logging
        self.logger = logger

        # for instantiating the client
        self.USERNAME = os.getenv("IG_USERNAME")
        self.PASSWORD = os.getenv("IG_PASSWORD")
        self.delay_range = delay_range
        self.session_json_path = session_json_path
        self.cl = self._login()

        # load geo json
        with open(city_geo_json_path, "r") as f:
            self.city_geo = json.load(f)

    def _login(self):
        """
        Attempts to login to Instagram using either the provided session information
        or the provided username and password.
        """

        cl = Client()
        session = cl.load_settings(self.session_json_path)

        login_via_session = False
        login_via_pw = False

        if session:
            try:
                cl.set_settings(session)
                cl.login(self.USERNAME, self.PASSWORD)

                # check if session is valid
                try:
                    cl.get_timeline_feed()
                    cl.delay_range = self.delay_range
                    return cl
                except LoginRequired:
                    self.logger.info(
                        "Session is invalid, need to login via username and password"
                    )

                    old_session = cl.get_settings()

                    # use the same device uuids across logins
                    cl.set_settings({})
                    cl.set_uuids(old_session["uuids"])

                    cl.login(self.USERNAME, self.PASSWORD)
                login_via_session = True
                cl.delay_range = self.delay_range
                return cl
            except Exception as e:
                self.logger.info(
                    "Couldn't login user using session information: %s" % e
                )

        if not login_via_session:
            try:
                self.logger.info(
                    "Attempting to login via username and password. username: %s"
                    % self.USERNAME
                )
                if cl.login(self.USERNAME, self.PASSWORD):
                    login_via_pw = True
            except Exception as e:
                self.logger.info(
                    "Couldn't login user using username and password: %s" % e
                )

        if not login_via_pw and not login_via_session:
            raise Exception("Couldn't login user with either password or session")

    def get_info_by_hashtags(
        self,
        hashtags: List[str],
        hashtags_master_dict: Dict[str, List[Dict[str, Any]]] = {},
        search_type: Literal["recent", "top"] = "top",
        amount: int = 10,
    ):
        """
        gets all relevant info for a list of hashtags

        Args:
            hashtags (List[str]): list of hashtags (without #)
                typically put locations
            hashtags_master_dict (Dict[str, List[Dict[str, Any]]], optional): dictionary of hashtags and their media info.
                defaults to {}.
            search_type (Literal["recent", "top"], optional): method to get the media.
                defaults to "top".
            amount (int, optional): amount of media to get.
                defaults to 100.

        Returns:
            Dict[str, List[Dict[str, Any]]]: dictionary of hashtags and their media info

        Usage (e.g.):
            hashtags_master_dict = {} (or previously generated/cached)
            ic = InstagramCrawler()
            location_hashtags = ["paris", "london", "newyork"]
            hashtags_master_dict = ic.get_info_by_hashtags(
                hashtags=location_hashtags,
                hashtags_master_dict=hashtags_master_dict,
            )
        """
        for hashtag in hashtags:
            # get the top hashtags
            if search_type == "top":
                media = self.cl.hashtag_medias_top(hashtag, amount)
            else:
                media = self.cl.hashtag_medias_recent(hashtag, amount)
            # get the relevant info from media
            hashtag_info = get_all_media_info(media)
            hashtags_master_dict[hashtag] = hashtag_info
        return hashtags_master_dict

    def get_info_by_location(
        self,
        city_name: str,
        location_master_dict: Dict[str, List[Dict[str, Any]]] = {},
        search_type: Literal["recent", "top"] = "top",
        amount: int = 10,
    ):
        """
        gets all relevant info for a location

        Args:
            city_name (str): name of the city
            location_master_dict (Dict[str, List[Dict[str, Any]]], optional): dictionary of location and their media info.
                defaults to {}.
            search_type (Literal["recent", "top"], optional): method to get the media.
                defaults to "top".
            amount (int, optional): amount of media to get.
                defaults to 10.

        Returns:
            location_master_dict: dictionary of location and their media info

        Usage (e.g.):
            location_master_dict = {} (or previously generated/cached)
            ic = InstagramCrawler()
            city_name = "paris"
            location_master_dict = ic.get_info_by_location(
                city_name=city_name,
                location_master_dict=location_master_dict,
            )
        """
        city_name = normalize_city_name(city_name=city_name)
        if city_name not in self.geo_json:
            self.logger.warning(f"city: {city_name} not found in geo json")
            return location_master_dict

        # get the location pk and location name
        city_dict = self.geo_json[city_name]
        lat, lng = city_dict["lat"], city_dict["lng"]
        loc_pk = get_location_pk(cl=self.cl, lat=lat, lng=lng)
        # get the medias for the location
        if search_type == "top":
            medias = self.cl.location_medias_top(loc_pk, amount)
        else:
            medias = self.cl.location_medias_recent(loc_pk, amount)
        # get the relevant info from media
        location_master_dict[city_name] = get_all_media_info(medias)
        return location_master_dict
