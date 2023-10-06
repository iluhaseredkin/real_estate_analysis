from keys import API_ID, API_HASH, phone_number
from telethon.sync import TelegramClient
import pandas as pd
import matplotlib.pyplot as plt
import re


# Путь к файлу с сессией
session_file = 'my_session'

# Список каналов для парсинга
channels_to_parse = ['batumi_arendaa', 'kobuletiarenda', 'arenda_v_tbilise', 'alaniya_arenda', 'stambyl_arenda', 'flattorentbelgrade', 'novisad_stan', 'kvartira_dom_arenda']

lim = 50000

def extract_prices(text):
    # Используем регулярное выражение для поиска чисел с символами валюты ($, USD, долл, €, евро, лир, TL, руб)
    prices = re.findall(r'\d+\s*(?:\$|USD|долл|€|евро|лир|TL|руб)', text)

    # Преобразуем найденные цены в числовой формат и фильтруем по заданному диапазону
    valid_prices = [float(re.sub(r'[^\d.]', '', price)) for price in prices if
                    100 < float(re.sub(r'[^\d.]', '', price)) < 15000]

    # Если найдено несколько цен в допустимом диапазоне, берем первую
    if valid_prices:
        return valid_prices[0]
    else:
        return None

# Функция для чтения сообщений из канала и сохранения их в таблицу с фильтрацией по ключевым словам
def read_channels_and_save_to_tables(channels, filter_keywords=None):
    with TelegramClient(session_file, API_ID, API_HASH) as client:
        # Авторизуемся
        client.start(phone_number)

        # Создаем пустой DataFrame для хранения всех сообщений из всех каналов
        all_messages_df = pd.DataFrame(columns=['ID', 'Дата', 'Ник', 'Стоимость', 'Валюта', 'Канал', 'Текст'])

        # Создаем список для хранения графиков
        fig, axs = plt.subplots(len(channels), figsize=(10, 6 * len(channels)))

        for i, channel_username in enumerate(channels):
            # Находим канал по его username
            channel_entity = client.get_entity(channel_username)

            # Получаем последние 100 сообщений из канала
            messages = client.get_messages(channel_entity, limit=lim)

            # Создаем пустой DataFrame для хранения сообщений
            messages_df = pd.DataFrame(columns=['ID', 'Дата', 'Ник', 'Стоимость', 'Валюта', 'Канал', 'Текст'])

            # Заполняем DataFrame сообщениями с фильтрацией по ключевым словам
            for message in messages:
                found_keyword = None
                if filter_keywords is not None and message.text and message.text.strip():  # Убедимся, что message.text не равен None и не пуст
                    for keyword in filter_keywords:
                        if keyword in message.text:
                            found_keyword = keyword
                            break  # Если хотя бы одно ключевое слово найдено, прерываем цикл
                    username = message.sender.username if message.sender and message.sender.username else 'N/A'

                    if not messages_df.empty:
                        messages_df = pd.concat([messages_df, pd.DataFrame(
                            {'ID': [message.id], 'Дата': [message.date], 'Ник': [username], 'Текст': [message.text],
                             'Валюта': [found_keyword]})], ignore_index=True)
                    else:
                        messages_df = pd.DataFrame(
                            {'ID': [message.id], 'Дата': [message.date], 'Ник': [username], 'Текст': [message.text],
                             'Валюта': [found_keyword]})

            # Сохраняем DataFrame в таблицу (в формате CSV) с именем, соответствующим названию канала
            messages_df = messages_df.drop_duplicates(subset=["Текст"])
            messages_df['Стоимость'] = messages_df['Текст'].apply(extract_prices)
            median_price = messages_df['Стоимость'].median()
            messages_df = messages_df[messages_df['Стоимость'] <= median_price * 2]
            filename = f'{channel_username}_data.csv'
            messages_df['Канал'] = channel_username
            messages_df = messages_df[[col for col in messages_df.columns if col != 'Текст'] + ['Текст']]
            min_data = messages_df['Дата'].min()
            min_data_str = min_data.strftime("%Y-%m-%d")
            axs[i].set_title("Анализ " + channel_username + " с " + min_data_str)

            messages_df.to_csv(filename, index=False)

            # Добавляем сообщения из канала в общий DataFrame
            all_messages_df = pd.concat([all_messages_df, messages_df], ignore_index=True)

            # Создаем гистограмму
            axs[i].hist(messages_df["Стоимость"], bins=20, edgecolor='k', alpha=0.7)
            axs[i].set_xlabel("Стоимость аренды в месяц")
            axs[i].set_ylabel("Количество предложений")
            axs[i].set_title("Анализ "+channel_username+" с "+min_data_str)

        # Сохраняем общий DataFrame в таблицу (в формате CSV, например)
        all_messages_df.to_csv('all_messages.csv', index=False)

        # Размещаем графики на одном листе
        plt.tight_layout()
        plt.savefig('all_arenda.png')
        plt.close()


if __name__ == '__main__':
    # filter_keywords = input("Введите ключевые слова для фильтрации (через запятую): ")
    filter_keywords = '$,USD,долл,EUR,€,евро,TL,лир'
    filter_keywords = [kw.strip() for kw in filter_keywords.split(',')]
    read_channels_and_save_to_tables(channels_to_parse, filter_keywords)
