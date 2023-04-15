class AggregateException(Exception):
    pass


class BadRequestException(AggregateException):
    def __str__(self):
        return f"Невалидный запрос. Пример запроса"
