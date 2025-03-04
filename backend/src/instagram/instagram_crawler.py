import os

from instagrapi import Client
from instagrapi.exceptions import LoginRequired
from typing import List, Dict, Literal

from src.services import logger


class InstagramCrawler:
    def __init__(
        self,
        delay_range: List[int] = [1, 3],
        session_json_path: str = "configs/session.json",
    ):
        # for logging
        self.logger = logger

        # for instantiating the client
        self.USERNAME = os.getenv("ACCOUNT_USERNAME")
        self.PASSWORD = os.getenv("ACCOUNT_PASSWORD")
        self.delay_range = delay_range
        self.session_json_path = session_json_path
        self.cl = self._login()

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

    def _get_media_info(self, media):
        """parses through media object to get relevant info"""
        return {
            "date": media.taken_at or "",
            "location": media.location or "",
            "caption": media.caption_text or "",
            "comment_count": media.comment_count or 0,
            "like_count": media.like_count or 0,
            "play_count": media.play_count or 0,
        }

    def _get_all_media_info(
        self,
        medias,
    ):
        """runs for a list of medias"""
        return [self._get_media_info(media) for media in medias]

    def get_info_by_hashtags(
        self,
        hashtags: List[str],
        classification: Literal["recent", "top"] = "top",
        amount: int = 100,
    ):
        """
        gets all relevant info for a list of hashtags

        Args:
            hashtags (List[str]): list of hashtags (without #)
                typically put locations
            classification (Literal["recent", "top"], optional): method to get the media.
                defaults to "top".
            amount (int, optional): amount of media to get.
                defaults to 100.

        Returns:
            Dict[str, List[Dict[str, Any]]]: dictionary of hashtags and their media info
        """
        media_dict = dict()
        for hashtag in hashtags:
            # get the top hashtags
            if classification == "top":
                media = self.cl.hashtag_medias_top(hashtag, amount)
            else:
                media = self.cl.hashtag_medias_recent(hashtag, amount)
            # get the relevant info from media
            all_hashtag_info = self._get_all_media_info(media)
            media_dict[hashtag] = all_hashtag_info
        return media_dict
