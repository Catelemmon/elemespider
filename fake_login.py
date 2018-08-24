# -*- coding: utf-8 -*-
"""
    Created by Catelemmon on 2018/7/14 0014
"""
import json
import random
import time
import codecs

from selenium import webdriver

__author__ = "Catelemmon"

chrome = webdriver.Chrome(executable_path='chromedriver.exe')


def login_eleme_by_password(account):
    """
    :param account: dict
    :return: List[dict]
    """
    chrome.get("https://h5.ele.me/login/#redirect=https://www.ele.me")
    time.sleep(random.randint(3, 5))
    chrome.find_element_by_link_text("密码登录").click()
    chrome.find_element_by_xpath("//section[@class='form-b6px1']/input[@type='text'and@autocapitalize='on']")\
        .send_keys(account["phone"])
    time.sleep(random.randint(1, 3))
    chrome.find_element_by_xpath("//section[@class='form-b6px1']/input[@type='password']")\
        .send_keys(account["password"])
    chrome.find_element_by_xpath("//button[@class='SubmitButton-2wG4T']").click()
    time.sleep(random.randint(3, 5))
    cookies = chrome.get_cookies()
    return cookies


def login_eleme_by_message(phone):
    """
    :param phone: int
    :return: list
    """
    chrome.get("https://h5.ele.me/login/#redirect=https://www.ele.me")
    time.sleep(random.randint(3, 5))
    chrome.find_element_by_xpath("//section[@class='MessageLogin-FsPlX']/input[@maxlength=11]").send_keys(phone)
    chrome.find_element_by_xpath(
        "//section[@class='MessageLogin-FsPlX']/input[@maxlength=11]/following-sibling::button").click()
    time.sleep(random.randint(1, 2))
    chrome.find_element_by_xpath("//section[@class='MessageLogin-FsPlX']/input[@maxlength=8]").send_keys(
        input("输入验证码: "))
    chrome.find_element_by_xpath("//button[@class='SubmitButton-2wG4T']").click()
    time.sleep(random.randint(3, 5))
    cookies = chrome.get_cookies()
    return cookies


if __name__ == '__main__':
    cookies_res = login_eleme_by_message("18280564538")
    with codecs.open("eleme_cookies.ini", "w", "utf-8") as fcookie:
        fcookie.write(json.dumps(cookies_res, ensure_ascii=False, indent="    "))