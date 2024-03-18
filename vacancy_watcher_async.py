#!/bin/python

import logging
import asyncio
import aiohttp
from logging.handlers import QueueHandler
from argparse import ArgumentParser
from os.path import getmtime
from bs4 import BeautifulSoup
from requests import get
from datetime import date, timedelta, datetime
from dateutil.parser import parse, parserinfo
from typing import Optional
from sqlalchemy import create_engine, select, inspect, ScalarResult
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, Session
from copy import deepcopy
from tableprinter import TablePrinter
from typing import Self, Callable
from re import compile
from multiprocessing import Process, Queue, Lock

# ======= работа с источниками ============
# Собираем данные по вакансии
class Vacancy:

    # задание сегодняшней даты и настроек хидеров раз и для всех экземпляров
    date_now = date.today()
    # headers для hh нужен из-а ddos защиты. Без него не выдает результат
    # для superjob чтобы исключить результаты из других регионов
    headers ={
        'get_hh_intermediate_data': {
            'cookie': ('cfidsgib-w-hh=ghtUNmALYo148wV9aXnXjwilr5M4IpNQ9+DI7j5XWFV1ja3Fp'
                'OCgGSNz0xUVl8Y1YBFm6wTzzlEfri/bORCfr7gYAUCINK5HwLbZlUQLCp5kJgrZN0vy2EQ'
                'V/ldnKk7QmAAaZ6ghHpGWV7EDS5teDFiviQnrYwOzEWTCLg=='),
            'user-agent': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.3'
                '6 (KHTML, like Gecko) Chrome/67.0.3396.87 Safari/537.36')
        },
        'get_superjob_intermediate_data': {
            'cookie': ('forceRemoteWorkDisabled=1'),
            'user-agent': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.3'
                '6 (KHTML, like Gecko) Chrome/67.0.3396.87 Safari/537.36')
            }
    }
    # таймаут для запросов, т.к. если не задать - пытаться будет бесконечно
    request_timeout = 20

    def __init__(
            self,
            source_type: str,
            title: str,
            link: str,
            company: str='',
            salary: str='',
            shortdesc: str='',
            date: date | str='',
            experience: str='',
            fulldesc: str='',
            ) -> None:
        # поскольку набор достапных сразу свойств у разных источников отличается,
        # обьявим все сразу
        self.source_type = source_type
        self.title = title
        self.company = company
        self.salary = salary
        self.shortdesc = shortdesc
        self.link = link
        self.date = date
        self.experience = experience
        self.fulldesc = fulldesc

    @staticmethod
    def get_element_or_empty(element: BeautifulSoup, selector: str) -> str:
        """Вспомогательная функция. Ищет элементы по заданным фильтрам (тип элемента,
        свойство), возвращает текст из элемента, или пустую строку"""
        return tmp.getText() if (tmp := element.select_one(selector)) else ''

    @classmethod
    def bad_status_code(cls, status_code: int, req_info: str, print_warn: bool = False) -> bool:
        """Проверяет статус код, пишел в лог если это не 200 и возвращает False,
        если все хорошо, и True в противном случае"""
        if status_code != 200:
            out_string = f'Статус код не 200, а {status_code}. Дополнительная информация: {req_info}'
            logger.warning(out_string)
            # сообщение в косоль о том, что данные, например для всего списка, не получены
            if print_warn:
                print(out_string)
            return True
        return False

    @classmethod
    def get_hh_intermediate_data(cls, days: int) -> list[Self]:
        """Проходится по всем страницам с результатом, выбирая все полезные данные"""
        result = []
        try:
            page = 0
            while True:
                one_page = get('https://kirov.hh.ru/search/vacancy?a'
                    'rea=49&enable_snippets=true&ored_clusters=true&professional_rol'
                    'e=156&professional_role=160&professional_role=10&professional_r'
                    'ole=12&professional_role=150&professional_role=25&professional_'
                    'role=165&professional_role=34&professional_role=36&professional'
                    '_role=73&professional_role=155&professional_role=96&professiona'
                    'l_role=164&professional_role=104&professional_role=157&professi'
                    'onal_role=107&professional_role=112&professional_role=113&profe'
                    'ssional_role=148&professional_role=114&professional_role=116&pr'
                    'ofessional_role=121&professional_role=124&professional_role=125'
                    f'&professional_role=126&search_period={days}&page={page}',
                    headers=cls.headers['get_hh_intermediate_data'], timeout=cls.request_timeout)
                # если ничего не получили, нечего и обрабаотывать
                if cls.bad_status_code(one_page.status_code, 'функция get_hh_intermediate_data', True):
                    return result
                one_page = BeautifulSoup(one_page.text, 'lxml')
                vacancy_items = one_page.find_all('div', "serp-item")
                # если даже на одной странице ничего нет, значит возвращаем пустой лист
                if not vacancy_items:
                    logger.info(f'Получен список из {len(result)} вакансий')
                    return result
                # Возвращаем словари с ключами: титул, зарплата, кампания, краткое описание, ссылка
                for vacancy in vacancy_items:
                    # ссылки на вакансию не должно не быть.. но разик случилось
                    # что сайт поменяли, так что защита
                    try:
                        link = vacancy.find('a', 'bloko-link')['href'].split('?')[0]
                    except TypeError:
                        link = 'Couldnt get a link'
                    result.append(Vacancy(
                        source_type = 'hh',
                        title = cls.get_element_or_empty(vacancy, 'a[class*=bloko-link]'),
                        salary = cls.get_element_or_empty(vacancy, 'span[data-qa="vacancy-serp__vacancy-compensation"]'),
                        company = cls.get_element_or_empty(vacancy, 'div[class*=vacancy-serp-item__meta-info-company]'),
                        link=link,
                        shortdesc = cls.get_element_or_empty(vacancy, 'div[class*=g-user-content]')
                    ))
                page += 1
        except Exception as e:
            print('Ошибка получения списка вакансий', e)
            logger.exception(f'Произошла ошибка при получении списка вакансий')
        return result

    @classmethod
    def get_trudkirov_intermediate_data(cls, days: int) -> list[Self]:
        """Запрашивает сразу страницу с 1000 результатов, столько все равно вряд ли будет.
        Используем простой requests, т.к. он тут работает и быстрее селениума"""
        result = []
        try:
            page = get(f'https://trudkirov.ru/vacancy/?WithoutAdditionalLimits=Fals'
                'e&ActivityScopeNoStandart=True&ActivityScope=97&SearchType=2&Region=43'
                '&AreaFiasOktmo=77612&HideWithEmptySalary=False&ShowOnlyWithEmployerInf'
                'o=False&ShowOnlyWithHousing=False&ShowChukotkaResidentsVacancies=False'
                '&ShowPrimorskAreaResident1Vacancies=False&ShowPrimorskAreaResident2Vac'
                'ancies=False&ShowPrimorskAreaResident3Vacancies=False&StartDate='
                f'{(cls.date_now - timedelta(days=days)).strftime("%d.%m.%Y")}&Sort=1&P'
                'ageSize=1000&SpecialCategories=False&IsDevelopmentProgram=False', timeout=cls.request_timeout)
            # если ничего не получили, нечего обрабатывать
            if cls.bad_status_code(page.status_code, 'get_trudkirov_intermediate_data', True):
                return result
            soup = BeautifulSoup(page.text, 'lxml')
            # Ищем таблицу с вакансиями. У нее нет отличительных аттрибутов, но на данный момент
            # она единственная содержит tbody на странице
            vacancies = soup.find('tbody')
            # Если 0 результатов, то будет таблица с данным классом в tr
            if vacancies is None or vacancies.select('.k-no-data'):
                return result
            vacancies = vacancies.find_all('tr')
            # Инициализируем элемент класса с полями: титул, зарплата, кампания, дата, ссылка
            for vacancy in vacancies:
                result.append(Vacancy(
                    source_type = 'trudkirov',
                    title = vacancy.contents[0].getText(),
                    salary = vacancy.contents[1].getText(),
                    company = vacancy.contents[3].getText(),
                    date = cls._date_from_string(vacancy.contents[4].getText(), 'trudkirov'),
                    # ссылки на вакансию не должно не быть. Также сократим её до тольконеобходимых данных
                    link = f"https://trudkirov.ru{vacancy.contents[0].find('a').attrs['href']}".partition('?returnurl=')[0],
                ))
            logger.info(f'Получен список из {len(result)} вакансий')
        except Exception:
            print('Ошибка получения списка вакансий')
            logger.exception(f'Произошла ошибка при получении списка вакансий')
        return result
    
    @classmethod
    def get_trudvsem_intermediate_data(cls, days: int) -> list[Self]:
        """Запрашивает данные с trudvsem. В отдельные дни сайт не умеет,
        может только день, три, неделя, месяц, все. Также не отдает более
        10 вакансий за раз. Более подробную информацию по вакансии получаем
        по api в дальнейшем"""
        def get_new_page(exp: str, page_num: int) -> dict | None:
            """Запрашивает страницу с заданными: количеством дней со дня
            публикации exp и номером страницы"""
            new_page = get('https://trudvsem.ru/iblocks/_catalog/flat_filter_prr_search_vacancies/data?'
                                'filter=%7B%22regionCode%22%3A%5B%224300000000000%22%5D%2C%22districts%22%3A'
                                '%5B%224300000100000%22%5D%2C%22professionalSphere%22%3A%5B%22InformationTec'
                                f'hnology%22%5D%2C%22publishDateTime%22%3A%5B%22{exp}%22%5D%7D&orderColumn=RE'
                                f'LEVANCE_DESC&page={page_num}&pageSize=10', timeout=cls.request_timeout)
            # если ничего не получили, нечего обрабатывать
            if cls.bad_status_code(new_page.status_code, 'get_trudvsem_intermediate_data', True):
                return None
            return new_page.json()            
        result = []
        match days:
            # 0 или 1, в общем сегодня
            case _ if days < 2:
                exp = 'EXP_0'
            # 3 дня
            case _ if days < 4:
                exp = 'EXP_1'
            # неделя
            case _ if days < 8:
                exp = 'EXP_2'
            # месяц. Не всегда будет точно, но вряд ли буду исползовать
            case _ if days < 32:
                exp = 'EXP_3'
            # все время
            case _:
                exp = 'EXP_MAX'
        try:
            # запросим цикл на 1000 страниц, вряд ли столько там будет
            for pg in range(100):
                page = get_new_page(exp, pg)
                # если плохой статус код или нет данных по вакансиям - на выход
                if page is None or not page['result']['data']:
                    return result
                # цикл по вакансиям на странице
                for vacancy in page['result']['data']:
                    result.append(Vacancy(
                        source_type = 'trudvsem',
                        title = vacancy[1],
                        company = vacancy[3],
                        date = datetime.fromtimestamp(int(str(vacancy[23])[:10])).date(),
                        link = f'https://trudvsem.ru/vacancy/card/{vacancy[2]}/{vacancy[0]}'
                    ))
                # Если страница последняя - выход
                if pg == page['result']['paging']['pages'] - 1:
                    break
            logger.info(f'Получен список из {len(result)} вакансий')
        except Exception:
            print('Ошибка получения списка вакансий')
            logger.exception(f'Произошла ошибка при получении списка вакансий')
        return result
    
    @classmethod
    def get_superjob_intermediate_data(cls, days: int) -> list[Self]:
        """Запрашивает данные с superjob, апи нет. В отдельные дни сайт также не
        умеет, можно запрашивать за один, три или семь дней. Если неверно
        указать дни, выдает непонятно что."""
        if days not in [1, 3, 7]:
            match days:
                # 0 или 1, в общем сегодня
                case _ if days < 2:
                    days = 1
                # 3 дня
                case _ if days < 4:
                    days = 3
                # неделя
                case _:
                    days = 7
        
        result = []
        try:
            # возмем по максимуму 5 страниц, вряд ли больше будет
            for pg in range(1, 6): 
                page = get('https://kirov.superjob.ru/vakansii/it-internet-svyaz-telekom/?period='
                            f'{days}&click_from=facet&page={pg}', headers=cls.headers['get_superjob_intermediate_data'],
                            timeout=cls.request_timeout)
                # если ничего не получили, нечего обрабатывать
                if cls.bad_status_code(page.status_code, 'get_trudvsem_intermediate_data', True):
                    return result
                soup = BeautifulSoup(page.text, 'lxml')
                yesterday_str = date.today() - timedelta(days=1)
                # немного про особенности сайта. Он выдает список результатов, где нужный регион просто
                # сверху, а дальше идут остальные, т.е. надо вовремя остановитсья.
                # также выдает рекламу типа "курс" или проплаченных вакансий
                # дата вакансии также приводится в виде "сегодня", "вчера"
                # большинство классов также автогенерированные, так что и зацепиться почти не за что
                # придется считать спаны
                vacancies = soup.find_all('div', {'class': 'f-test-search-result-item'})
                # пустая страница
                if not vacancies:
                    break
                for vacancy in vacancies:
                    # пропустим проплаченную вакансию, у нее зеленая обводка, заданная стилем
                    if vacancy.find('div', attrs={'style': compile(r'background-color*')}) is not None:
                        continue
                    # у первого спана нет узнаваемого аттрибута, но он важен, т.к. содержит дату или курс
                    first_span = vacancy.find('span')
                    # если по какой-то причине нет ни одного спана - нам брать там нечего
                    if first_span is None:
                        continue
                    vacancy_date = first_span.getText()
                    # курс - просто реклама, "Вакансии из соседних городов" - просто надпись
                    # остальные даты преобразовываем в объект
                    match vacancy_date:
                        case 'Курс' | 'Вакансии из соседних городов':
                            continue
                        case _ if 'Сегодня' in vacancy_date:
                            vacancy_date = cls.date_now
                        case 'Вчера':
                            vacancy_date = yesterday_str
                        case _:
                            vacancy_date = cls._date_from_string(vacancy_date, 'superjob')
                    city = cls.get_element_or_empty(vacancy, 'span[class*=f-test-text-company-item-location]')
                    # если киров кончился - останов
                    # если нет города - очередная реклама
                    if not city:
                        continue
                    if 'Киров (Кировская область)' not in city:
                        break
                    # также, если вышли за заданную дату - тоже останов
                    if vacancy_date < cls.date_now - timedelta(days=days):
                        break
                    title_and_link = vacancy.find('a')
                    this_vacancy = Vacancy(
                        source_type = 'superjob',
                        title = title_and_link.getText(),
                        link = f'https://kirov.superjob.ru{title_and_link.attrs["href"]}'
                    ) 
                    this_vacancy.salary = cls.get_element_or_empty(vacancy, 'div[class*=f-test-text-company-item-salary]')
                    this_vacancy.company = cls.get_element_or_empty(vacancy, 'span[class*=f-test-text-vacancy-item-company-name]')
                    this_vacancy.date = vacancy_date
                    # поскольку опереться почти не на что, то будем собирать от кнопки "подать резюме"
                    # но уйдя повыше на 5 родительских элементов, и вверх до слова Киров
                    if (proper_parent := vacancy.find('button', attrs={'class': 'f-test-button-Otkliknutsya'})) is not None:
                        proper_parent = proper_parent.parent.parent.parent.parent.parent
                        # нужно получить текст от его двух предыдущих сиблингов и частично от
                        # предпредыдущего. Максимум таких сиблингов 3, но на всякий случай возмем 4
                        # и вовремя остановимся
                        for _ in range(3):
                            proper_parent = proper_parent.previousSibling
                            if (bages := proper_parent.find_all('span', attrs={'class': 'f-test-badge'})) and bages is not None:
                                this_vacancy.shortdesc = '. '.join([ bage.getText() for bage in bages ]) + '. ' + this_vacancy.shortdesc
                                break
                            this_vacancy.shortdesc = proper_parent.getText() + this_vacancy.shortdesc
                    result.append(this_vacancy)
            logger.info(f'Получен список из {len(result)} вакансий')
        except Exception:
            print('Ошибка получения списка вакансий')
            logger.exception(f'Произошла ошибка при получении списка вакансий')
        return result
                  
    # локализуем парсер
    class _rus_parserinfo(parserinfo):
        MONTHS = [
            ('янв', 'января'),
            ('фев', 'февраля'),      
            ('мар', 'марта'),
            ('апр', 'апреля'),
            ('май', 'мая'),
            ('июн', 'июня'),
            ('июл', 'июля'),
            ('авг', 'августа'),
            ('сен', 'сент', 'сентября'),
            ('окт', 'октября'),
            ('ноя', 'ноября'),
            ('дек', 'декабря')
        ]

    @classmethod
    def _date_from_string(cls, somedate: str, source: str) -> date:
        """Ищет в строке дату и пытается её распарсить в datetime объект"""
        # пытаемся получить datetime объект. Если не получилось, то возвращаем текущую дату
        # а ошибку просто в лог
        try:
            date = parse(parserinfo=cls._rus_parserinfo(), timestr=somedate, fuzzy=True).date()
            # иногда бывает что с датами на сайтах ошибаются и ставят из будущего
            # это ломает логику, так что берем седняшную дату вместо этого
            if date <= cls.date_now:
                return date
        except Exception:
            logger.info(f'Произошла ошибка при конвертации даты. Полученная строка - "{somedate}", сайт-источник - "{source}"')
        return cls.date_now
    # все методы, которые пойдут в параллельные процессы
    @classmethod
    def methods(cls) -> list[Callable]:
        """Возвращает все имеющиеся методы, предназначенные для получения данных"""
        return [
            cls.get_hh_intermediate_data,
            cls.get_trudvsem_intermediate_data,
            cls.get_superjob_intermediate_data,
            cls.get_trudkirov_intermediate_data
        ]

async def get_one_vacancy(session: aiohttp.ClientSession, queue: asyncio.Queue) -> None:
    """Запрашивает и парсит полные данные по частично заполненной вакансии,
    не возвращает ничего, т.к. дописывает в класс"""
    while True:
        # запрос элемента класса Vacancy из очереди
        one_vacancy = await queue.get()
        try:
            # trudvsem особый случай
            # поскольку ссылка на вакансию для меня и для компа отличается (json),
            # сделаем из ссылки на страницу, ссылку на json в api
            # ссылка на читаемую страницу https://trudvsem.ru/vacancy/card/1027700404797/0cd46ee2-0b4d-11ee-81f4-dbfed3997e57
            # ссылка на получение json http://opendata.trudvsem.ru/api/v1/vacancies/vacancy/1027700404797/0cd46ee2-0b4d-11ee-81f4-dbfed3997e57
            if one_vacancy.source_type == 'trudvsem':
                link = f'http://opendata.trudvsem.ru/api/v1/vacancies/vacancy/{one_vacancy.link.split("card/")[-1]}'
            else:
                link = one_vacancy.link
            # асинхронный запрос страницы
            async with session.get(link, allow_redirects=False, timeout=20) as response:
                # trudvsem исключение, там мы получаем json
                if one_vacancy.source_type == 'trudvsem':
                    page = await response.json()
                # в остальных случаях html, который нужно парсить
                else:
                    page = await response.text()
                    soup = BeautifulSoup(page, 'lxml')
                # если ничего не получили, нечего обрабатывать
                if one_vacancy.bad_status_code(response.status, f'get_one_vacancy | source type is {one_vacancy.source_type}'):
                    # поскольку дата нужна для записи в БД, то в случае неполучения данных по вакансии, нужно
                    # недостающее заполнить
                    if not one_vacancy.date:
                        one_vacancy.date = one_vacancy.date_now
                    return
            # в зависимости от источника ищем разные элементы страницы
            match one_vacancy.source_type:
                case 'hh':
                    one_vacancy.experience = one_vacancy.get_element_or_empty(soup, 'span[data-qa*=vacancy-experience]')
                    one_vacancy.fulldesc = one_vacancy.get_element_or_empty(soup, 'div[data-qa*=vacancy-description]')
                    one_vacancy.date = one_vacancy._date_from_string(one_vacancy.get_element_or_empty(soup, 'p[class*=vacancy-creation-time-redesigned] > span'), 'hh')
                case 'trudkirov':
                    dts = soup.find_all('dt')
                    description = {
                        'duties': '',
                        'additional': ''
                    }
                    for dt in dts:
                        match dt.getText():
                            case 'Стаж': one_vacancy.experience = dt.find_next_sibling('dd').getText()
                            case 'Должностные обязанности': description['duties'] = f"Должностные обязанности: {dt.find_next_sibling('dd').getText()}"
                            case 'Дополнительные пожелания': description['additional'] = f"Дополнительные пожелания: {dt.find_next_sibling('dd').getText()}"
                    one_vacancy.fulldesc = '\n'.join(description.values())
                    one_vacancy.shortdesc = description['duties'] if len(description['duties']) < 400 else description['duties'][:400]
                case 'trudvsem':
                    page = page['results']['vacancies']
                    if len(page) > 1:
                        logger.warning(f'По ссылке {link} пришло несколько вакансий')
                    elif len(page) < 1:
                        logger.warning(f'По ссылке {link} не пришло вакансий')
                        return
                    one_vacancy.salary = page[0]['vacancy']['salary']
                    one_vacancy.fulldesc = BeautifulSoup(page[0]['vacancy']['duty'], 'lxml').getText()
                    one_vacancy.shortdesc = one_vacancy.fulldesc if len(one_vacancy.fulldesc) < 400 else one_vacancy.fulldesc[:400]
                    one_vacancy.experience = page[0]['vacancy']['requirement']['experience']
                    # one_vacancy.date = one_vacancy._date_from_string(page[0]['vacancy']['creation-date'])
                case 'superjob':
                    # из дополнительной информации можно подчерпнуть только опыт работы и полное описание
                    # оно обычно идет после class="f-test-address", если есть
                    # если ничего не получили, нечего обрабатывать
                    # найдем адрес (регион)
                    city = soup.find('div', attrs={'class': 'f-test-address'})
                    if city is not None:
                        features = city.nextSibling
                        if features is not None:
                            # Опыт работы не требуется, неполный рабочий день, удалённая работа
                            features = features.getText()
                            # добавим их в полное описание
                            one_vacancy.fulldesc = features
                            # вычленим опыт, если имеется
                            features = features.split(',')
                            for feature in features:
                                if 'опыт' in feature.lower():
                                    one_vacancy.experience = feature
                                    break
                    # найдем полное описание. описание вообще всего находится в div с классом
                    # f-test-vacancy-base-info, интересующее нас описание - во втором потомке
                    # второго его потомка
                    base_info = soup.find('div', attrs={'class': 'f-test-vacancy-base-info'})
                    if base_info is not None and len(base_info.contents) > 2:
                        second_sibling = base_info.contents[1]
                        if len(second_sibling.contents) > 2:
                            one_vacancy.fulldesc += second_sibling.contents[1].getText()
        except Exception:
            logger.warning(f'Для вакансии {one_vacancy.link} не удалось получить подробных данных', exc_info=True)
        finally:
            # отмечаем задачу сделанной
            queue.task_done()
            # небольшая задержка дабы не ddos-ить
            await asyncio.sleep(0.2)

async def proccess_worker(method: Callable, days: int) -> list[Vacancy] | None:
    """Функция для обработки отдельным процессом. Независимая.
    Собирает промежуточные данные синхронным requests,
    после чего собирает все оставшиеся данные асинхронно,
    по 5 запросов за раз (почти за раз)"""
    # синхронное получение общего списка вакансий
    vacancy_list = method(days)
    # если пусто - нечего обрабатывать
    if not vacancy_list:
        return vacancy_list
    # очередь, чтобы ограничить количество одновременных запросов
    queue = asyncio.Queue()
    # заполняем очередь сразу всеми данными
    for item in vacancy_list:
        queue.put_nowait(item)
    # создаем сессию, которой передаем headers по имени метода, либо None
    async with aiohttp.ClientSession(headers=Vacancy.headers.get(method.__name__)) as session:
        # создаем пять потребителей - корутин которые почти одновременно
        # будут ожидать ответа
        consumers = [ asyncio.create_task(get_one_vacancy(session, queue)) for _ in range(5) ]
        # ждем пока все задания в очереди будут готовы
        await queue.join()
    # завершаем все потребители, т.к. они стоят на бесконечном цикле ожидания
    # новых данных из очереди
    for task in consumers:
        task.cancel()
    return vacancy_list

def process_starter(method: Callable, days: int, console: Lock, session: Session) -> None:
    """Нужна только для того, чтобы запустить асинхронную
    корутину на выполнение. Заодно занимается записью в бд
    и выводом результата"""
    # получение данных с сайта
    result = asyncio.run(proccess_worker(method, days))
    # прогон через бд
    result = db_writer(days, result, session)
    # вывод в консоль
    # захват консоли
    console.acquire()
    table_writer(result)
    # отдаем консоль
    console.release()

# # ======== БД =======================
# Базовый класс. Просто нужен для ORM
class Base(DeclarativeBase):

    # добавим возможность сравнивать экземпляры класса на равенство по аттрибутам, кроме id
    def __eq__(self, other):
        classes_match = isinstance(other, self.__class__)
        a, b = deepcopy(self.__dict__), deepcopy(other.__dict__)
        #compare based on equality our attributes, ignoring SQLAlchemy internal stuff
        a.pop('_sa_instance_state', None)
        a.pop('id', None)
        b.pop('_sa_instance_state', None)
        b.pop('id', None)
        # приведем значения обоих инстансов к строкам, чтобы исключить ситуацию,
        # когда из бд получаем строку, а с сайтов - число
        for item in [a, b]:
            for k, v in item.items():
                item[k] = str(v)
        attrs_match = (a == b)
        return classes_match and attrs_match

    def __ne__(self, other):
        return not self.__eq__(other)

# Класс вакансии для БД. С Optional - строки, в которые можно и не получить информацию из источников
class VacancyDB(Base):
    __tablename__ = 'vacancies'
    id: Mapped[int] = mapped_column(primary_key=True)
    source_type: Mapped[str]
    title: Mapped[str]
    company: Mapped[str]
    salary: Mapped[Optional[str]]
    shortdesc: Mapped[Optional[str]]
    link: Mapped[str]
    date: Mapped[date]
    experience: Mapped[Optional[str]]
    fulldesc: Mapped[Optional[str]]

def db_reader(days: int, session: Session) -> ScalarResult:
    """Запрашивает из БД данные за указанное количество дней.
    Предполагается, что сессия подключения к БД создана заранее"""
    return session.scalars(select(VacancyDB).where(VacancyDB.date >= (date.today() - timedelta(days=days))))

def db_writer(days: int, vacancy_list: list[Vacancy], session: Session) -> list[VacancyDB]:
    """Запрашивает из БД вакансии за указанное количество дней,
    сравнивает с vacancy_list и удаляет дубликаты. После,
    дописывает новые вакансии в БД и выдает отфильтрованный список,
    без дубликатов, встреченных в БД. Функция предполагается к
    использованию в потоках, поэтому предполагается,
    что сессия подключения к БД создана заранее"""
    # если на входе пустой лист - делать ничего не надо
    if not vacancy_list:
        return []
    # преобразуем в класс sqlalchemy
    vacancies_db = [ VacancyDB(**item.__dict__) for item in vacancy_list ]
    # сохраним источник вакансий, вдруг все отфильтруем
    source_type = vacancies_db[0].source_type
    # дубликаты - вакансии, уже лежащие в бд
    duplicates = []
    # запросим нужные данные из бд
    recent_rows = db_reader(days, session)
    for item in recent_rows:
        # теперь нужно сравнить каждый экземпляр в ответе из бд с каждым из источников и выявить дубликаты
        for raw_vacancy in vacancies_db:
            if raw_vacancy == item:
                duplicates.append(raw_vacancy)
    # удаляем дубликаты
    vacancies_db = [ item for item in vacancies_db if not item in duplicates ]
    logger.info(f'Отфильтровано {len(duplicates)} дубликатов, полученных из {source_type}')
    # записываем в бд только свежие данные
    session.add_all(vacancies_db)
    session.commit()
    return vacancies_db

def table_writer(vacancy_list: list[VacancyDB]) -> None:
    """Выводит данные из VacancyDB итемов на экран"""
    # параметры табличного вывода
    try:
        headers = [('title', 15), ('company', 10), ('salary', 10), 'shortdesc', ('date', 10), ('experience', 5), ('link', 100)]
        table = []
        # осталось только распотрошить классы на словари и отправить на вывод
        # но при этом исключить одинаковые вакансии на случай, если запрос производится
        # за большой период. будем выводить только те, у кого дата свежее
        # а одинаковые выяснять по ссылке
        for item in vacancy_list:
            for ready_item in table:
                if item.link == ready_item['link']:
                    if item.date > ready_item['date']:
                        ready_item['date'] = item.date
                        break
            else:
                table.append({c.key: getattr(item, c.key) for c in inspect(item).mapper.column_attrs})
        logger.info(f'Отброшено {len(vacancy_list) - len(table)} повторяющихся "свежих" вакансий')
        logger.info(f'Получено {len(table)} записей для вывода')
        tableprint = TablePrinter(headers, table, header_size_matters=True)
        tableprint.printer()
    except Exception:
        logger.exception('tibleprinter вернул ошибку.', exc_info=True)

def logger_process(queue: Queue) -> None:
    """Отдельный процесс, который будет записывать данные в лог,
    когда скрипт переходит на параллельное выполнение (запрос и обработка
    данных с разных сайтов), берет из очереди, пишей в файл"""
    # создаем новый логгер
    logger = logging.getLogger()
    # configure a stream handler
    logger.addHandler(logging.FileHandler(filename='vw.log', encoding='utf-8'))
    # log all messages, debug and up
    logger.setLevel(logging.DEBUG)
    # run forever
    while True:
        # consume a log message, block until one arrives
        message = queue.get()
        # log the message
        if message is None:
            break
        logger.handle(message)

# Скрипт, запущенный без аргументов, выбирает данные с сайтов за последние сутки.
# аргументы db и web с последующим числом укажут на то, откуда сделать выборку и за какой период
if __name__ == '__main__':
    # настройки логгера
    # logging.basicConfig(
    #     filename='vw.log',
    #     encoding='utf-8',
    #     style='{',
    #     format='{asctime} {funcName} [{levelname}] - {message}',
    #     datefmt="%Y.%m.%d %H:%M:%S",
    #     level=logging.INFO)
    # аргументы
    parser = ArgumentParser(
        description='Позволяет запросить и вывести новые вакансии с сайтов, либо сохраненные из бд',
        prog='vw',
        epilog='Вызов без параметров предполагает источник - web и количество дней зависит от даты модификации файла sqlite'
        )
    parser.add_argument('source', choices=['db', 'web'], nargs='?', default='web', help='Нужно выбрать тип источника')
    parser.add_argument('days', type=int, nargs='?', help='Дней для запроса с сайтов или бд', default=1)
    args = parser.parse_args()
    # получаем текущий логгер
    # очередь, куда процессы будут кидать свои логи
    logger_queue = Queue()
    # берем корневой логгер
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    handler = QueueHandler(logger_queue)
    handler.setFormatter(logging.Formatter(
        '{asctime} {funcName} [{levelname}] - {message}', "%Y.%m.%d %H:%M:%S", style='{'
    ))
    logger.addHandler(handler)
    logger.info(f'Запуск с параметрами: source {args.source}, days {args.days}')
    # sqlite БД
    bd_file = 'vacancy.db'
    engine = create_engine(f'sqlite+pysqlite:///{bd_file}')
    # Создает файл БД с таблицами. Если уже создано - не затирает ничего.
    Base.metadata.create_all(engine)
    if args.days is None:
        # Для начала нужно проверить, когда файл бд менялся последний раз, дабы запросить из источников
        # вакансии за этот период +1 день, на всякий случай
        timespan = (date.today() - date.fromtimestamp(getmtime(bd_file))).days + 1
    else:
        # или берем то, что запросил пользователь явно
        timespan = args.days
    with Session(engine) as session:
        # запрос с сайтов
        if args.source == 'web':
            # Дабы не выводить вакансии с разных источников вразнобой, консоль
            # нужно на время вывода получать эксклюзивно
            console = Lock()
            # создаем процессы для всех методов получения первоначальных данных
            processes = [ Process(target=process_starter, args=(method, timespan, console, session)) for method in Vacancy.methods() ]
            # логирующий процесс
            logger_p = Process(target=logger_process, args=(logger_queue,))
            # запускаем на исполнение
            logger_p.start()
            for process in processes:
                process.start()
            for process in processes:
                process.join(timeout=240)
            logger_queue.put(None)
            if logger_p.is_alive():
                # после завершения всех процессов, если логгер не завершился - завершим его
                logger_p.terminate()
        else:
            # запрос из бд
            table_writer(db_reader(timespan, session))
