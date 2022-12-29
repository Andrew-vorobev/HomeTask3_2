import os
import csv


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
            os.replace(file_name, 'years\\' + file_name)


data = Separator('vacancies_by_year.csv')
data.split_csv()
