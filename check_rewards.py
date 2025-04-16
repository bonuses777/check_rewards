import requests
import json
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

# Скрипт получает из блокчейна статистику rewards и benwallet
# в указанном диапазоне времени

wallet_path = "wallet.txt" # укажите файл txt со списком проверяемых public key 
base_url = "https://openapi.nkn.org/api/v1/blocks"
filter_date_start = datetime.strptime("2025-04-16 23:59:59", "%Y-%m-%d %H:%M:%S")  # Начальная дата
filter_date_finish = datetime.strptime("2025-04-16 00:00:01", "%Y-%m-%d %H:%M:%S")  # Конечная дата
page = 1 # Start page. Check in browser https://openapi.nkn.org/api/v1/blocks?per_page=250&page=1
per_page = 250  # Количество элементов на странице

# # Открываем файл ссо списком проверяемых public key
with open(wallet_path, 'r') as wallet_file:
    wallets = wallet_file.read().splitlines()

next_page_url = f"{base_url}?per_page={per_page}&page={page}"

def process_block(block):
    created_at = datetime.strptime(block["header"]["created_at"], "%Y-%m-%d %H:%M:%S")
    wallet = block["header"]["signerPk"]

    # Проверяем, что дата не младше заданной
    if created_at >= filter_date_start:
        return None

    # Проверяем, что дата не старше заданной
    if created_at <= filter_date_finish:
        return None

    # # Проверяем, совпадает ли кошелек с данными из файла
    if wallet in wallets:
        return block["header"]

    return None

with ThreadPoolExecutor(max_workers=10) as executor:  # Количество рабочих потоков (подстройте по своим нуждам)
    while next_page_url:
        # Отправляем GET-запрос к серверу
        response = requests.get(next_page_url, headers={"Content-Type": "application/json", "Accept": "application/json"})

        # Проверяем успешность запроса
        if response.status_code != 200:
            print(f"Ошибка при запросе: {response.status_code}")
            break

        # Получаем JSON-ответ
        data = json.loads(response.text)

        # Итерируемся по блокам и обрабатываем их параллельно
        results = list(executor.map(process_block, data["blocks"]["data"]))

        # Выводим результаты
        for result in results:
            if result is not None:
                #print("Совпадение найдено!")
                #print("Header:", result)

                # Извлекаем необходимые данные
                height = result['height']                
                created_at = result['created_at']
                signer_pk = str(result['signerPk'])
                beneficiary_wallet = result['benificiaryWallet']

                print(f"Block height: {height}")
                print(f"Created at: {created_at}")
                print(f"Signer Public Key: {signer_pk}")
                print(f"Beneficiary wallet: {beneficiary_wallet}")
                print()
                
        # Увеличиваем номер страницы для следующего запроса
        page += 1
        next_page_url = f"{base_url}?per_page={per_page}&page={page}"
        print(next_page_url)

        # Проверяем, если дата последнего блока стала старше заданной, завершаем выполнение
        last_block_created_at = datetime.strptime(data["blocks"]["data"][-1]["header"]["created_at"], "%Y-%m-%d %H:%M:%S")
        print(last_block_created_at)
        if last_block_created_at <= filter_date_finish:
            break

print("Завершено.")