from os import get_terminal_size
from math import ceil, floor

# Класс для представления таблицы из бд в виде таблицы из псевдографики
# Стандартные средства не умеют объединять ячейки
class TablePrinter:
    # К столбцам будем относить пробелы вокруг текста и черту справа от текста
    # Один же символ резервируем под самую левую черту
    terminal_size = get_terminal_size().columns - 1
    # количество добавочных к каждой колонке символов, чтобы нарисовать таблицу
    # пробел перед контентом, пробел после контента и |
    column_frame = 3

    # В headers предполагается массив ключей типа ['aaa', 'bbb', ('ccc', 5), ('ddd', 100)],
    # где кортежы пределяют ширину ячейки в процентах от терминала. 100% ячейка будет
    # вынесена под более мелкие ячейки и займер всю ширину
    def __init__(
            self, headers: list,
            body: list,
            header_rename: dict = {},
            header_size_matters: bool = False,
            shrink_cols_to_content: bool = True
            ) -> None:
        # Наименования всех колонок
        self._headers = headers
        # Массив словарей - строк таблицы
        self.body = []
        # Приведем все к строкам, дабы можно было работать и длинами строк, а также
        # уберем случайные переходы на новую строку. Изменим еще названия колонок,
        # если таковые заданы
        for row in body:
            stringified_dict = {}
            for k, v in row.items():
                stringified_dict[k] = str(v).replace('\n', '')
            self.body.append(stringified_dict)
        # если понадобится переназвать заголовки
        self.header_rename = header_rename
        # при расчете ширины колонки, будет браться в расчет как ширина
        # контента, так и ширина всех заголовков - родного и переименованного
        self.header_size_matters = header_size_matters
        # при расчете ширины колонки, она будет сужаться, если
        # контент занимает меньше, чем задано
        self.shrink_cols = shrink_cols_to_content

    @classmethod
    def _percent_to_value(cls, value: int) -> int:
        """Переводит ширину колонки в процентах от ширины терминала
        в ширину в символах. 1 - минимальная допустимая длина колонки,
        контент будет в одну букву"""

        return result if (result := floor(value * cls.terminal_size / 100)) > 1 else 1

    def _get_content_max_width(self, name: str, predefined_width: int) -> int:
        """Пользователь может задать ширину колонки в процентах,
        но если её контент влезет и в меньшую ширину - нет смысла оставлять место.
        Функция корректирует ширину заданную в символах в реально необходимую
        predefined - то, что было задано в процентах изначально"""

        # если учитывается длина контента заголовка
        if self.header_size_matters:
            # то вычисляем длину данного заголовка в символах с учетом переименования
            # если оно есть
            if (renamed_header := self.header_rename.get(name)) is not None:
                header = [len(renamed_header)]
            else:
                header = [len(name)]
        else:
            header = []
        content_width = max([ len(k[name]) for k in self.body ] + header)
        # может получиться, что колонка пуста, а запись всего одна. Дабы сохранить структуру
        # таблицы, даже пустая строка должна присутствовать. Поэтому дадим ей один символ
        if content_width == 0:
            content_width = 1
        if content_width < predefined_width:
            self._row_shrunk = True
            return content_width
        return predefined_width

    def _get_content_max_lines(self, row: dict) -> dict:
        """Считает, сколько строк понадобится, чтобы вывести весь контент
        заданной строки при вычисленных длинах её колонок."""

        # all - количество колонок обычных строк, не полноширинных
        rows_length = {'all': 0}
        # отвильтруем лишние данные
        filtered_row = [ (k, v) for k, v in row.items() if k in list(self.headers.keys()) + self.full_size_rows ]
        for k, v in filtered_row:
            # если колонка - полноширинная
            if k in self.full_size_rows:
                # считаем её по отношению к ширине всего терминала
                rows_length[k] = ceil(len(v) / (self.terminal_size ))
            # если обычная, то делим длину контента на отведенную длину, из которой 3 символа зарезервированы
            elif (item_len := ceil(len(v) / self.headers[k])) > rows_length['all']:
                # возвращаем максимальное значение строк, получившееся в какой либо из колонок
                rows_length['all'] = item_len
        return rows_length

    def _share_reminder(self, reminder: int, cols: list) -> int | None:
        """Делит остаток строки поровну между колонками без заранее заданной
        ширины. Если какая то строка при этом окажется меньше, то выкидывает
        её и делит заново"""

        # так как функция рекурсивная, то сначала нужно проверить, есть ли еще элементы на входе
        if not cols:
            return
        # сбросим флаг
        self._row_shrunk = False
        # ширина колонок
        width = floor(reminder/len(cols))
        for item in cols:
            # проверяем соответствие заявленных ширин требуемым
            self.headers[item] = self._get_content_max_width(item, width)
            # если колонка изменила размер - убираем её и считаем заново для тех что остались
            if self._row_shrunk:
                cols.remove(item)
                self._share_reminder(reminder - self.headers[item], cols)
                return



    def _get_lengths(self) -> None:
        """Вычисляет реальные длины колонок в символах,
        расфасовывает по категориям"""

        # колонки с длинной контента 100%
        self.full_size_rows = []
        # колонкис их длинами в символах
        self.headers = {}
        # остаток свободного места в строке. Пока вся строка в нашем распоряжении
        reminder = self.terminal_size
        # строки у которых не указана ширина
        cols_without_width = []
        # строки у которых указана ширина
        cols_with_width = []
        # сохраним изначальную последовательность полей кроме полноширинных
        self.sequence = []
        for item in self._headers:
            # указана ширина
            if isinstance(item, tuple):
                # проверка кортежа на правильный формат
                if not 1 <= (length := len(item)) <= 2:
                    raise ValueError(
                        ('Кортеж описания столбца может содержать заголовок, '
                        f'либо заголовок и ширину в процентах. Получен кортеж длинной {length}')
                        )
                name, *width = item
            # не уазана ширина
            elif isinstance(item, str):
                name = item
                width = []
            else:
                # ни строка и ни кортеж - непонятно что
                raise ValueError(
                    ('headers - это набор строк заголовков колонок, либо '
                    'кортежей, где также указан размер колонки в процентах ширины')
                    )
            match width:
                # полноширинная колонка
                case [100]:
                    self.full_size_rows.append(name)
                # колонка без указания ширины
                case []:
                    self.headers[name] = None
                    cols_without_width.append(name)
                    self.sequence.append(name)
                # колонка с заданной шириной
                case _:
                    if not isinstance(width[0], int):
                        raise ValueError(
                            (f'Размер колонки указан, но не является цифровым целочисленным значением - {width[0]},'
                            'что недопустимо. Укажите число!')
                        )
                    cols_with_width.append(item)
                    self.sequence.append(name)
        # вычислим необходимое количество служебных символов, и зарезервируем их
        reminder -= self.column_frame * (len(cols_with_width) + len(cols_without_width))
        # вычисляем ширины колонок с заданными ширинами
        # item - это кортеж (str: name, num: width)
        for name, width in cols_with_width:
            self.headers[name] = self._get_content_max_width(name, self._percent_to_value(width))
            # из остатка линии вычитаем длину этой строки
            reminder -= self.headers[name]
            # если остаток линии ушел в минус - значит пользователь напутал в процентах. 146 тут не катит
            if reminder < 0:
                raise ValueError('Общая сумма процентов ширины колонок получилась больше 100, так быть не должно')
        # проверим, хватит ли места на все оставшиеся колонки, хотябы по одному символу в строку
        if cols_without_width:
            width_of_cols_without_width = floor(reminder/len(cols_without_width))
            if width_of_cols_without_width < 1:
                raise ValueError('Для колонок, с неуказанной шириной, осталось места менее 1 символа на каждую. Этого мало')
        # вычислим ширины всех оставшихся строк
        # если учитываем ширину контента, то вычисляем рекурсивнно
        if self.shrink_cols:
            self._share_reminder(reminder, cols_without_width)
        else:
        # если нет, то просто поровну
            for name in cols_without_width:
                self.headers[name] = width_of_cols_without_width

        # вычислим новый размер терминала. Реальный размер терминала не изменится, но таблица могла
        # ужаться, в соответствии с данными
        self.terminal_size = sum(self.headers.values()) + self.column_frame * (len(self.headers) - len(self.full_size_rows))
    
    def _assemble_str_line(self, name: str, content: str) -> str:
        """Прибавляет к строке content очередную ячейку, огороженную табличным оформлением"""

        return f' {content}{" " * (self.headers[name] - len(content))} |'

    def printer(self) -> None:
        """Основная функция. Делает красиво"""

        if self.body:
            # Посчитаем параметры будущей таблицы
            self._get_lengths()
            # горизонтальная черта
            row_splitter = '-' * (sum(self.headers.values()) + 1 + self.column_frame * len(self.headers))
            print(row_splitter)
            # заголовки - такая же строка для вывода. Нужно их присоединить в нчала общего массива с данными
            header_dict = { k: self.header_rename.get(k, k) for k in self.headers.keys() }
            # перебираем массив данных
            for item in [header_dict] + self.body:
                # количество линий для данной строки бд
                item_lines = self._get_content_max_lines(item)
                for line in range(item_lines['all']):
                    # левая сторона таблицы
                    assembly_line = '|'
                    # контент во всех колонках, кроме полноширинных, бьем на куски, чтобы влезли в линию, и собираем линию
                    # необходимо также сохранить первоначальную последовательность колонок,
                    # поэтому будем идти по self._headers исключая полноширинные, но брать значения из headers
                    for name in self.sequence:
                        # смещение в строке для каждой новой линии
                        line_shift = line * self.headers[name]
                        assembly_line += self._assemble_str_line(
                            name, item[name][line_shift : line_shift + self.headers[name]] if line != item_lines['all'] - 1 else item[name][line_shift :]
                        )
                    print(assembly_line)
                print(row_splitter)
                # для полноширинных все то же самое, только без функции сборки линии
                for full_row in self.full_size_rows:
                    # пропуск заголовка, такие колонки его не имеют
                    if item.get(full_row, None) is not None:
                        for line in range(item_lines[full_row]):
                            line_shift = line * (self.terminal_size - 3)
                            if line == item_lines[full_row] - 1:
                                assembly_line = f'| {item[full_row][line_shift : ]}{" " * (self.terminal_size - len(item[full_row][line_shift : ]))} |'
                            else:
                                assembly_line = f'| {item[full_row][line_shift : line_shift + self.terminal_size]} |'
                            print(assembly_line)
                        print(row_splitter)
        else:
            print('Нет строк для вывода')

# table = TablePrinter(
#     ['aaa', 'bbb', ('ccc', 45), ('ddd', 100), 'dsdssasasasas'],
#     body=[{
#         'aaa': '123efd',
#         'bbb': '123456789123456789123456789',
#         'ccc': 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
#         'ddd': 'sdsssssssssssssssssss654nmbvzfjkhvdfvhbfdzvbhfdzvhdzvbhfdvhdssss',
#         'dsdssasasasas': 'sa'
#     }, {
#         'aaa': '',
#         'bbb': '12345678',
#         'ccc': 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
#         'ddd': '1234567891234567891239123456789',
#         'dsdssasasasas': '12'
#     }],
#     header_rename={'bbb': '111111'},
#     header_size_matters=True,
#     shrink_cols_to_content=True)
# table.printer()
