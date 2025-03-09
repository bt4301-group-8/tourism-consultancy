import random
import time
from enum import Enum

from instagrapi.mixins.challenge import ChallengeResolveMixin
from instagrapi.exceptions import (
    ChallengeError,
    ChallengeSelfieCaptcha,
    ChallengeUnknownStep,
)

WAIT_SECONDS = 5


class ChallengeChoice(Enum):
    SMS = 0
    EMAIL = 1


def extract_messages(challenge):
    messages = []
    for item in challenge["extraData"].get("content"):
        message = item.get("title", item.get("text"))
        if message:
            dot = "" if message.endswith(".") else "."
            messages.append(f"{message}{dot}")
    return messages


def get_code_from_sms(username):
    while True:
        code = input(f"Enter code (6 digits) for {username}: ").strip()
        if code and code.isdigit():
            return code
    return False


class ChallengeResolver(ChallengeResolveMixin):
    def challenge_resolve_simple(self, challenge_url: str) -> bool:
        """
        Old type (through private api) challenge resolver
        Помогите нам удостовериться, что вы владеете этим аккаунтом

        Parameters
        ----------
        challenge_url : str
            Challenge URL

        Returns
        -------
        bool
            A boolean value
        """
        step_name = self.last_json.get("step_name", "")
        if step_name == "delta_login_review" or step_name == "scraping_warning":
            # IT WAS ME (by GEO)
            self._send_private_request(challenge_url, {"choice": "0"})
            return True
        elif step_name == "add_birthday":
            random_year = random.randint(1970, 2004)
            random_month = random.randint(1, 12)
            random_day = random.randint(1, 28)
            self._send_private_request(
                challenge_url,
                {
                    "birthday_year": str(random_year),
                    "birthday_month": str(random_month),
                    "birthday_day": str(random_day),
                },
            )
            return True
        elif step_name in ("verify_email", "verify_email_code", "select_verify_method"):
            if step_name == "select_verify_method":
                """
                {'step_name': 'select_verify_method',
                'step_data': {'choice': '0',
                'fb_access_token': 'None',
                'big_blue_token': 'None',
                'google_oauth_token': 'true',
                'vetted_device': 'None',
                'phone_number': '+7 *** ***-**-09',
                'email': 'x****g@y*****.com'},     <------------- choice
                'nonce_code': 'DrW8V4m5Ec',
                'user_id': 12060121299,
                'status': 'ok'}
                """
                steps = self.last_json["step_data"].keys()
                challenge_url = challenge_url[1:]
                if "email" in steps:
                    self._send_private_request(
                        challenge_url, {"choice": ChallengeChoice.EMAIL}
                    )
                elif "phone_number" in steps:
                    self._send_private_request(
                        challenge_url, {"choice": ChallengeChoice.SMS}
                    )
                else:
                    raise ChallengeError(
                        f'ChallengeResolve: Choice "email" or "phone_number" '
                        f"(sms) not available to this account {self.last_json}"
                    )
            wait_seconds = 5
            for attempt in range(24):
                code = self.challenge_code_handler(self.username, ChallengeChoice.EMAIL)
                if code:
                    break
                time.sleep(wait_seconds)
            print(
                f'Code entered "{code}" for {self.username} ({attempt} attempts by {wait_seconds} seconds)'
            )
            self._send_private_request(challenge_url, {"security_code": code})
            # assert 'logged_in_user' in client.last_json
            assert self.last_json.get("action", "") == "close"
            assert self.last_json.get("status", "") == "ok"
            return True
        elif step_name == "":
            assert self.last_json.get("action", "") == "close"
            assert self.last_json.get("status", "") == "ok"
            return True
        elif step_name == "change_password":
            # Example: {'step_name': 'change_password',
            #  'step_data': {'new_password1': 'None', 'new_password2': 'None'},
            #  'flow_render_type': 3,
            #  'bloks_action': 'com.instagram.challenge.navigation.take_challenge',
            #  'cni': 18226879502000588,
            #  'challenge_context': '{"step_name": "change_password",
            #      "cni": 18226879502000588, "is_stateless": false,
            #      "challenge_type_enum": "PASSWORD_RESET"}',
            #  'challenge_type_enum_str': 'PASSWORD_RESET',
            #  'status': 'ok'}
            wait_seconds = 5
            for attempt in range(24):
                pwd = self.change_password_handler(self.username)
                if pwd:
                    break
                time.sleep(wait_seconds)
            print(
                f'Password entered "{pwd}" for {self.username} ({attempt} attempts by {wait_seconds} seconds)'
            )
            return self.bloks_change_password(pwd, self.last_json["challenge_context"])
        elif step_name == "selfie_captcha":
            raise ChallengeSelfieCaptcha(self.last_json)
        elif step_name == "select_contact_point_recovery":
            """
            {
                'step_name': 'select_contact_point_recovery',
                'step_data': {'choice': '0',
                    'phone_number': '+62 ***-****-**11',
                    'email': 'g*******b@w**.de',
                    'hl_co_enabled': False,
                    'sigp_to_hl': False
                },
                'flow_render_type': 3,
                'bloks_action': 'com.instagram.challenge.navigation.take_challenge',
                'cni': 178623487724,
                'challenge_context': '{"step_name": "select_contact_point_recovery",
                "cni": 178623487724,
                "is_stateless": false,
                "challenge_type_enum": "HACKED_LOCK",
                "present_as_modal": false}',
                'challenge_type_enum_str': 'HACKED_LOCK',
                'status': 'ok'
            }
            """
            steps = self.last_json["step_data"].keys()
            challenge_url = challenge_url[1:]
            if "email" in steps:
                self._send_private_request(
                    challenge_url, {"choice": ChallengeChoice.EMAIL}
                )
            elif "phone_number" in steps:
                self._send_private_request(
                    challenge_url, {"choice": ChallengeChoice.SMS}
                )
            else:
                raise ChallengeError(
                    f'ChallengeResolve: Choice "email" or "phone_number" (sms) '
                    f"not available to this account {self.last_json}"
                )
            wait_seconds = 5
            for attempt in range(24):
                code = self.challenge_code_handler(self.username, ChallengeChoice.EMAIL)
                if code:
                    break
                time.sleep(wait_seconds)
            print(
                f'Code entered "{code}" for {self.username} ({attempt} attempts by {wait_seconds} seconds)'
            )
            self._send_private_request(challenge_url, {"security_code": code})

            if self.last_json.get("action", "") == "close":
                assert self.last_json.get("status", "") == "ok"
                return True

            # last form to verify account details
            assert (
                self.last_json["step_name"] == "review_contact_point_change"
            ), f"Unexpected step_name {self.last_json['step_name']}"

            # details = self.last_json["step_data"]

            # TODO: add validation of account details
            # assert self.username == details['username'], \
            #     f"Data invalid: {self.username} does not match {details['username']}"
            # assert self.email == details['email'], \
            #     f"Data invalid: {self.email} does not match {details['email']}"
            # assert self.phone_number == details['phone_number'], \
            #     f"Data invalid: {self.phone_number} does not match {details['phone_number']}"

            # "choice": 0 ==> details look good
            self._send_private_request(challenge_url, {"choice": 0})

            # TODO: assert that the user is now logged in.
            # # assert 'logged_in_user' in client.last_json
            # assert self.last_json.get("action", "") == "close"
            # assert self.last_json.get("status", "") == "ok"
            return True
        elif step_name == "submit_phone":
            code = get_code_from_sms(self.username)
            steps = self.last_json["step_data"].keys()
            challenge_url = challenge_url[1:]
            self._send_private_request(challenge_url, {"choice": ChallengeChoice.SMS})
            return True
        else:
            raise ChallengeUnknownStep(
                f'ChallengeResolve: Unknown step_name "{step_name}" for '
                f'"{self.username}" in challenge resolver: {self.last_json}'
            )
        return True
