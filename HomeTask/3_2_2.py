import csv
import os
from multiprocessing import Pool
import time


naming = 0
minimum = 1
maximum = 2
money = 3
place = 4
timePub = 5

CURRENCY = {
    "AZN": 35.68,
    "BYR": 23.91,
    "EUR": 59.90,
    "GEL": 21.74,
    "KGS": 0.76,
    "KZT": 0.13,
    "RUR": 1,
    "UAH": 1.64,
    "USD": 60.66,
    "UZS": 0.0055,
}


class Separator:
    def __init__(self, file_name):
        file_csv = open(file_name, encoding='utf_8_sig')
        reader_csv = list(csv.reader(file_csv))
        self.title, self.vacancies = reader_csv[0], reader_csv[1:]

    def split_csv(self):
        path = r'C:\Users\vorobov\PycharmProjects\HomeTask\years'
        if not os.path.exists(path):
            os.makedirs(path)
        for f in os.listdir(path):
            os.remove(os.path.join(path, f))

        n = 0
        files_by_year = {}
        while n < len(self.vacancies):
            year = self.vacancies[n][5][:4]
            file_name = year + '.csv'
            file = open(file_name, 'a', newline='', encoding="utf-8")
            writer = csv.writer(file)
            writer.writerow(self.title)
            while n < len(self.vacancies) and year == self.vacancies[n][5][:4]:
                writer.writerow(self.vacancies[n])
                n += 1
            file.close()
            files_by_year[int(year)] = 'years\\' + file.name
            os.replace(file_name, 'years\\' + file_name)
        return files_by_year


class DataSet:
    def __init__(self, fileName, name):
        self.vacancies_Lenght = 0
        separator = Separator(fileName)
        self.files_by_year = separator.split_csv()
        self.name = name
        with open(fileName, encoding='utf-8') as e:
            reader = csv.reader(e)
            data = list(reader)
            title = data[0]
            self.naming = title.index("name")
            self.minimum = title.index("salary_from")
            self.maximum = title.index("salary_to")
            self.money = title.index("salary_currency")
            self.place = title.index("area_name")
            self.publishedAt = title.index("published_at")
            self.data = data[1:]

    def one_year_data(self, item):
        value = False
        fileName = item[1]
        with open(fileName, encoding='utf-8') as e:
            reader = csv.reader(e)
            sum = length = amount = currencyLenght = 0
            for string in reader:
                if not value:
                    value = True
                    continue
                else:
                    salaryCurr = (int(float(string[maximum])) + int(float(string[minimum]))) * CURRENCY[string[money]] // 2
                    title = string[naming]
                    sum += salaryCurr
                    length += 1
                    if self.name in title:
                        amount += salaryCurr
                        currencyLenght += 1
        return item[0], sum, length, amount, currencyLenght

    def year_statistic(self):
        result = []
        for item in self.files_by_year.items():
            result.append(self.one_year_data(item))
        return {x[0]: x[1] for x in result}, {x[0]: x[2] for x in result}, {x[0]: x[3] for x in result}, {x[0]: x[4] for x in result}

    def year_statistic_with_mp(self):
        p = Pool()
        result = p.map(self.one_year_data, self.files_by_year.items())

        p.close()
        p.join()

        return {x[0]: x[1] for x in result}, {x[0]: x[2] for x in result}, {x[0]: x[3] for x in result}, {x[0]: x[4] for x in result}

    def get_data(self):
        start_time = time.time()
        year_data = self.year_statistic()
        print("--- %s seconds ---" % (time.time() - start_time))

        start_time = time.time()
        year_data = self.year_statistic_with_mp()
        print("--- %s seconds ---" % (time.time() - start_time))

        year = [i for i in range(2007, 2023)]
        sum = year_data[0]
        lenght = year_data[1]
        amount = year_data[2]
        currencyLenght = year_data[3]
        for i in year:
            sum[i] = int(sum[i] // lenght[i])
            amount[i] = int(amount[i] // currencyLenght[i])

        cities = []
        citiesAmount = {}
        citiesLenght = {}
        for string in self.data:
            newString = string.copy()
            if all(newString):
                salaryCurr = (int(float(string[maximum])) + int(float(string[minimum]))) * CURRENCY[string[money]] // 2
                city = string[place]
                if city not in cities:
                    cities.append(city)
                citiesAmount[city] = citiesAmount.get(city, 0) + salaryCurr
                citiesLenght[city] = citiesLenght.get(city, 0) + 1
                self.vacancies_Lenght += 1
        for i in cities:
            citiesAmount[i] = int(citiesAmount[i] // citiesLenght[i])
        need = [city for city in cities if citiesLenght[city] >= self.vacancies_Lenght // 100]
        ans = {key: citiesAmount[key] for key in sorted(need, key=lambda x: citiesAmount[x], reverse=True)[:10]}
        partitions = {key: float("{:.4f}".format(citiesLenght[key] / self.vacancies_Lenght)) for key in
                      sorted(need, key=lambda x: citiesLenght[x] / self.vacancies_Lenght, reverse=True)[:10]}

        print("Динамика уровня зарплат по годам:", sum)
        print("Динамика количества вакансий по годам:", lenght)
        print("Динамика уровня зарплат по годам для выбранной профессии:", amount)
        print("Динамика количества вакансий по годам для выбранной профессии:", currencyLenght)
        print("Уровень зарплат по городам (в порядке убывания):", ans)
        print("Доля вакансий по городам (в порядке убывания):", partitions)


if __name__ == '__main__':
    data = DataSet('vacancies_by_year.csv', 'Аналитик')
    data.get_data()