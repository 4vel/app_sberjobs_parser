import logging
from dbsrc import DataAccessLayer, Vacancy
from config import conn_string, admins, BOT_TOKEN
from parser import VacancyParser
from sqlalchemy.exc import DBAPIError
from messagist import send_message


def clean_table(dal):
    try:
        dal.connect()
        num_rows_deleted = dal.session.query(Vacancy).delete()
        logging.info(f'Удаляю данные Vacancy. Удалено {num_rows_deleted}')
        dal.session.commit()
    except DBAPIError as exc:
        logging.info(f'{exc}')
        dal.session.rollback()


def parse_vacancies():
    vp = VacancyParser()
    vp.get_vacancies()
    vp.show_info()
    vacancy_objects = vp.get_list_of_vacancies_obj()
    return vacancy_objects


def insert_into_db(dal, vacancy_objects):
    """ """

    try:
        dal.connect()
        logging.info("Сохраняю вакансии в БД")
        dal.session.bulk_save_objects(vacancy_objects)
        dal.session.commit()

    except DBAPIError as exc:
        logging.info(f'{exc}')
        dal.session.rollback()


def load_vacancies_pipeline():
    """ Пайплайн обновления вакансий """

    try:
        logging.info("Начинаю пайплайн парсинга и загрузки вакансий")

        dal = DataAccessLayer(conn_string)
        clean_table(dal)
        vacancy_objects = parse_vacancies()
        insert_into_db(dal, vacancy_objects)

        send_message(admins[0], BOT_TOKEN, "Вакансии обновлены")

    except KeyError as err:
        logging.info(err)


def health_check():
    print("💟")
